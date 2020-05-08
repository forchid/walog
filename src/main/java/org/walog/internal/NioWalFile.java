/**
 * The MIT License (MIT)
 * <p>
 * Copyright (c) 2020 little-pan
 * <p>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * <p>
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.walog.internal;

import org.walog.CorruptWalException;
import org.walog.Releaseable;
import org.walog.Wal;
import org.walog.util.IoUtils;
import org.walog.util.LruCache;
import org.walog.util.WalFileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Integer.getInteger;
import static org.walog.util.WalFileUtils.lastFileLsn;

public class NioWalFile implements  AutoCloseable, Releaseable {

    protected static final String PROP_BLOCK_CACHE_SIZE = "org.walog.block.cacheSize";
    protected static final int BLOCK_CACHE_SIZE = getInteger(PROP_BLOCK_CACHE_SIZE, 16);
    protected static final int BLOCK_SIZE = 4 << 10;
    protected static final int WAL_MIN_SIZE = 1 + 8;
    protected static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;

    protected final File file;
    protected final long lsn;
    protected final RandomAccessFile raf;
    protected final FileChannel chan;
    protected final LruCache<Integer, Block> readCache;
    protected long filePos;
    private final long initSize;
    private ByteBuffer writeBuffer;

    private volatile boolean open;
    private final AtomicInteger refCount;

    public NioWalFile(File file) throws IOException {
        this(file, BLOCK_CACHE_SIZE);
    }

    public NioWalFile(File file, int blockCacheSize) throws IOException {
        this.file  = file;
        this.lsn   = WalFileUtils.lsn(file.getName());
        this.raf   = new RandomAccessFile(file, "rw");
        this.refCount  = new AtomicInteger();
        this.readCache = new LruCache<>(blockCacheSize);

        boolean failed = true;
        try {
            this.chan = this.raf.getChannel();
            this.initSize = isLastFile()? -1: this.chan.size();
            this.open = true;
            failed = false;
        } finally {
            if (failed) {
                IoUtils.close(this.raf);
            }
        }
    }

    public String getFilename() {
        return this.file.getName();
    }

    public File getFile() {
        return this.file;
    }

    protected ByteBuffer getWriteBuffer() {
        if (this.writeBuffer == null) {
            this.writeBuffer = wrapBuffer(new byte[BLOCK_SIZE]);
        }

        return this.writeBuffer;
    }

    protected void resetWriteBuffer() {
        ByteBuffer buf = this.writeBuffer;

        if (buf != null) {
            buf.clear();
        }
    }

    public long getLsn() {
        return this.lsn;
    }

    public long size() throws IOException {
        long size = this.initSize;
        return  (size == -1? this.chan.size(): size);
    }

    public boolean isLastFile() {
        final File dir = getFile().getParentFile();
        final String filename = getFilename();
        final long lastLsn = lastFileLsn(dir, filename);
        return (getLsn() == lastLsn);
    }

    protected byte getByte(final int offset) throws IOException {
        final int pageOffset = offset / BLOCK_SIZE;
        final int blockOffset= offset % BLOCK_SIZE;
        Block block = this.readCache.get(pageOffset);
        ByteBuffer buf;

        if (block == null || (buf = block.buffer()) == null) {
            final int pos = pageOffset * BLOCK_SIZE;
            final int rem = (int)(size() - pos);
            if (blockOffset +1 > rem) {
                throw new EOFException();
            }
            buf = ByteBuffer.allocate(Math.min(BLOCK_SIZE, rem));
            IoUtils.readFully(this.chan, buf, pos);
            if (buf.limit() == BLOCK_SIZE) {
                this.readCache.put(pageOffset, new Block(buf));
            }
        }

        return buf.get(blockOffset);
    }

    protected void getBytes(final int offset, byte[] buffer) throws IOException {
        getBytes(offset, buffer, 0, buffer.length);
    }

    protected void getBytes(final int offset, byte[] buffer, int i, int len)
            throws IOException {
        final int pageOffset = offset / BLOCK_SIZE;
        final int blockOffset= offset % BLOCK_SIZE;
        Block block = this.readCache.get(pageOffset);
        ByteBuffer buf;

        if (block == null || (buf = block.buffer()) == null) {
            final int pos = pageOffset * BLOCK_SIZE;
            final int rem = (int)(size() - pos);
            if (blockOffset + len > rem) {
                throw new EOFException();
            }
            buf = ByteBuffer.allocate(Math.min(BLOCK_SIZE, rem));
            IoUtils.readFully(this.chan, buf, pos);
            if (buf.limit() == BLOCK_SIZE) {
                this.readCache.put(pageOffset, new Block(buf));
            }
        }

        final int lim = buf.limit();
        int n = Math.min(lim - blockOffset, len);
        for (int j = blockOffset, k = blockOffset + n; j < k; ++j) {
            buffer[i++] = buf.get(j);
        }
        if (n < len) {
            getBytes(offset + n, buffer, i, len - n);
        }
    }

    public SimpleWal get(final long lsn) throws IOException {
        if (this.lsn != WalFileUtils.fileLsn(lsn)) {
            throw new IllegalArgumentException("lsn not in this file: " + lsn);
        }
        return get(WalFileUtils.fileOffset(lsn));
    }

    /** Query specified offset wal.
     *
     * @param offset the wal offset in this file
     * @return wal, or null if offset bigger than or equals to size of this file
     * @throws IOException if IO error
     */
    public SimpleWal get(final int offset) throws IOException {
        if (offset + WAL_MIN_SIZE > size()) {
            return null;
        }

        // wal format: Length(var-int), Data, Offset(int), Data checksum(int)
        int length, i = 0;
        final int p = getByte(offset + i++) & 0xff;
        if (p < 0xfb) {
            length = p;
        } else if (p == 0xfc) {
            length  =  getByte(offset + i++) & 0xff;
            length |= (getByte(offset + i++) & 0xff) << 8;
        } else if (p == 0xfd) {
            length  =  getByte(offset + i++) & 0xff;
            length |= (getByte(offset + i++) & 0xff) << 8;
            length |= (getByte(offset + i++) & 0xff) << 16;
        } else {
            final String message = "Illegal prefix of wal length: " + Integer.toHexString(p);
            throw new CorruptWalException(message, this.file.getAbsolutePath(), offset);
        }

        final byte[] data = new byte[length];
        getBytes(offset + i, data);
        i += data.length;

        final byte[] intArr = new byte[4];
        final ByteBuffer intBuf = wrapBuffer(intArr);
        getBytes(offset + i, intArr);
        i += intArr.length;
        final int offsetStored = intBuf.getInt(0);
        if (offsetStored != offset) {
            throw new CorruptWalException("Offset not matched", this.file.getAbsolutePath(), offset);
        }

        getBytes(offset + i, intArr);
        final int chkSum = intBuf.getInt(0);
        if (chkSum != IoUtils.getFletcher32(data)) {
            throw new CorruptWalException("Checksum error", this.file.getAbsolutePath(), offset);
        }

        return new SimpleWal(this.lsn | offset, (byte)p, data);
    }

    private ByteBuffer wrapBuffer(byte[] buffer) {
        ByteBuffer buf = ByteBuffer.wrap(buffer);
        buf.order(BYTE_ORDER);
        return buf;
    }

    public void append(List<AppendPayloadItem> items) throws IOException {
        this.filePos = this.chan.size();
        this.chan.position(this.filePos);

        final int n = items.size();
        int i = 0, flushIndex = 0;
        while (i < n) {
            AppendPayloadItem item = items.get(i);
            if (!item.isCompleted()) {
                flushIndex = append(items, item, i, flushIndex);
            }
            ++i;
        }

        flush(items, i, flushIndex);
        for (AppendPayloadItem item : items) {
            if (item.wal != null) {
                item.setResult(item.wal);
            }
        }
    }

    protected int append(List<AppendPayloadItem> items, AppendPayloadItem item, int i, int flushIndex)
            throws IOException {

        final byte[] payload = item.payload;
        final int length = payload.length;
        final int headSize;
        final byte pfx;

        if (length >= 1 << 24) {
            throw new IllegalArgumentException("payload too big");
        }
        if (length >= 1 << 16) {
            headSize = 4;
            pfx = (byte)0xfd;
        } else if (length >= 0xfb) {
            headSize = 3;
            pfx = (byte)0xfc;
        } else {
            headSize = 1;
            pfx = (byte)length;
        }
        // wal format: Length(var-int), Data, Offset(int), Data checksum(int)
        final int walSize  = headSize + payload.length + 8;
        if (this.filePos + walSize > Wal.LSN_OFFSET_MASK) {
            throw new IOException(this.file.getAbsolutePath() + " full");
        }

        final int offset = (int)this.filePos;
        final int chkSum = IoUtils.getFletcher32(payload);
        final ByteBuffer buffer = getWriteBuffer();
        flushIndex = writeHeader(items, i, flushIndex, buffer, headSize, pfx, length);
        flushIndex = write(items, i, flushIndex, buffer, payload);
        flushIndex = writeInt(items, i, flushIndex, buffer, offset);
        flushIndex = writeInt(items, i, flushIndex, buffer, chkSum);
        this.filePos += walSize;

        final long lsn = this.lsn | offset;
        item.wal = new SimpleWal(lsn, pfx, payload);

        return flushIndex;
    }

    private int writeHeader(List<AppendPayloadItem> items, final int i, int flushIndex,
                             ByteBuffer buffer, int headSize, byte pfx, int length) throws IOException {

        if (headSize == 1) {
            flushIndex = write(items, i, flushIndex, buffer, (byte)length);
        } else if (headSize == 3) {
            flushIndex = write(items, i, flushIndex, buffer, pfx);
            flushIndex = write(items, i, flushIndex, buffer, (byte)(length));
            flushIndex = write(items, i, flushIndex, buffer, (byte)(length >> 8));
        } else if (headSize == 4) {
            flushIndex = write(items, i, flushIndex, buffer, pfx);
            flushIndex = write(items, i, flushIndex, buffer, (byte)(length));
            flushIndex = write(items, i, flushIndex, buffer, (byte)(length >> 8));
            flushIndex = write(items, i, flushIndex, buffer, (byte)(length >> 16));
        } else {
            throw new IllegalArgumentException("headSize " + headSize);
        }

        return flushIndex;
    }

    protected int writeInt(List<AppendPayloadItem> items, final int index, int flushIndex,
                            ByteBuffer buffer, int i) throws IOException {
        if (buffer.remaining() < 4) {
            flushIndex = flush(items, index, flushIndex);
        }
        buffer.putInt(i);

        return flushIndex;
    }

    protected int write(List<AppendPayloadItem> items, int i, int flushIndex,
                         ByteBuffer buffer, byte[] bytes) throws IOException {
        for (final byte b : bytes) {
            flushIndex = write(items, i, flushIndex, buffer, b);
        }
        return flushIndex;
    }

    protected int write(List<AppendPayloadItem> items, final int i, int flushIndex,
                         ByteBuffer buffer, byte b) throws IOException {
        if (!buffer.hasRemaining()) {
            flushIndex = flush(items, i, flushIndex);
        }
        buffer.put(b);

        return flushIndex;
    }

    protected int flush(List<AppendPayloadItem> items, final int i, int flushIndex)
            throws IOException {

        flush();
        while (flushIndex < i) {
            AppendPayloadItem item = items.get(flushIndex);
            if (item.wal != null) {
                item.flushed = true;
            }
            ++flushIndex;
        }

        return flushIndex;
    }

    protected void flush() throws IOException {
        final ByteBuffer buffer = this.writeBuffer;
        final int p = buffer.position();
        final int n = p / BLOCK_SIZE;
        int lim = BLOCK_SIZE;

        buffer.flip();
        for (int i = 0; i < n; ++i) {
            buffer.limit(lim);
            while (buffer.hasRemaining()) {
                this.chan.write(buffer);
            }
            buffer.position(lim);
            lim += BLOCK_SIZE;
            buffer.limit(p);
        }

        final int rem = p % BLOCK_SIZE;
        if (rem > 0) {
            while (buffer.hasRemaining()) {
                this.chan.write(buffer);
            }
        }

        resetWriteBuffer();
    }

    public void recovery() throws IOException {
        final long size = this.size();
        if (size < WAL_MIN_SIZE) {
            this.chan.truncate(0L);
            return;
        }

        // Check integrity
        int offset;
        // 1. First back forward
        {
            final byte[] buf = new byte[4];
            final int p = (int)(size - 8);
            getBytes(p, buf);
            offset = wrapBuffer(buf).getInt(0);
            try {
                Wal wal = get(offset);
                IoUtils.debug("walog last lsn 0x%x in '%s'", wal.getLsn(), this.file);
                this.chan.position(size);
                return;
            } catch (CorruptWalException |EOFException e) {
                IoUtils.error("walog exit abnormally, recovery ...", e);
                offset = 0;
            }
        }
        // 2. Otherwise forward
        SimpleWal wal = null;
        try {
            while (offset < size) {
                wal = get(offset);
                offset = wal.nextOffset();
            }
        } catch (final EOFException e) {
            final String message;
            if (wal == null) {
                message = String.format("walog corrupted and recovery from 0x%x in '%s'",
                        offset, this.file);
            } else {
                message = String.format("walog corrupted and recovery from 0x%x in '%s', last lsn 0x%x",
                        offset, this.file, wal.getLsn());
            }
            IoUtils.error(message, e);
            this.chan.truncate(offset);
            this.chan.position(offset);
        }
    }

    public void sync() throws IOException {
        this.chan.force(false);
    }

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close()  {
        IoUtils.close(this.readCache);
        IoUtils.close(this.chan);
        IoUtils.close(this.raf);

        this.open = false;
    }

    @Override
    public void retain(int n) {
        this.refCount.addAndGet(n);
    }

    @Override
    public void retain() {
        this.refCount.incrementAndGet();
    }

    @Override
    public void release() {
        if (this.refCount.decrementAndGet() <= 0) {
            close();
        }
    }

}
