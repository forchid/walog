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

import org.walog.CorruptedException;
import org.walog.Wal;
import org.walog.util.IoUtils;
import org.walog.util.LruCache;
import org.walog.util.WalFileUtils;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class NioWalFile implements  AutoCloseable {
    protected static final int BLOCK_SIZE = 4 << 10;

    protected final File file;
    protected final long lsn;
    protected final RandomAccessFile raf;
    protected final FileChannel chan;
    protected final LruCache<Integer, Block> blockCache;

    private volatile boolean open;

    public NioWalFile(File file, int blockCacheSize) throws IOException {
        this.file  = file;
        this.lsn   = WalFileUtils.lsn(file.getName());
        this.raf   = new RandomAccessFile(file, "rw");
        this.blockCache = new LruCache<>(blockCacheSize);

        boolean failed = true;
        try {
            this.chan = this.raf.getChannel();
            this.open = true;
            failed = false;
        } finally {
            if (failed) {
                IoUtils.close(this.raf);
            }
        }
    }

    public long getLsn() {
        return this.lsn;
    }

    public long size() throws IOException {
        return this.chan.size();
    }

    protected byte getByte(final int offset) throws IOException {
        final int pageOffset = offset / BLOCK_SIZE;
        final int blockOffset= offset % BLOCK_SIZE;
        Block block = this.blockCache.get(pageOffset);
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
                this.blockCache.put(pageOffset, new Block(buf));
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
        Block block = this.blockCache.get(pageOffset);
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
                this.blockCache.put(pageOffset, new Block(buf));
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

    public Wal get(final int offset) throws IOException {
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
            throw new CorruptedException(message, this.file.getAbsolutePath(), offset);
        }

        final byte[] data = new byte[length];
        getBytes(offset + i, data);
        i += data.length;

        final byte[] ia = new byte[4];
        getBytes(offset + i, ia);
        i += ia.length;
        final int offsetStored = IoUtils.readInt(ia);
        if (offsetStored != offset) {
            throw new CorruptedException("Offset not matched", this.file.getAbsolutePath(), offset);
        }

        getBytes(offset + i, ia);
        i += ia.length;
        final int checksum = IoUtils.readInt(ia);
        if (checksum != IoUtils.getFletcher32(data)) {
            throw new CorruptedException("Checksum error", this.file.getAbsolutePath(), offset);
        }

        return new SimpleWal(this.lsn | offset, (byte)p, data);
    }

    public boolean append(byte[] payload) throws IOException {
        final int length = payload.length;
        final byte[] head;
        int i = 0;

        if (length >= 24 << 10) {
            throw new IllegalArgumentException("payload too big");
        }
        if (length >= 16 << 10) {
            head = new byte[4];
            head[i++] = (byte)0xfd;
            head[i++] = (byte)(length);
            head[i++] = (byte)(length >> 8);
            head[i++] = (byte)(length >> 16);
        } else if (length >= 0xfb) {
            head = new byte[3];
            head[i++] = (byte)0xfc;
            head[i++] = (byte)(length);
            head[i++] = (byte)(length >> 8);
        } else {
            head = new byte[1];
            head[i++] = (byte)length;
        }

        final int checksum = IoUtils.getFletcher32(payload);
        final ByteBuffer buffer = ByteBuffer.allocate(i + payload.length + 8);
        buffer.put(head).put(payload);

        synchronized (this.chan) {
            final long pos = this.chan.position();
            if (pos + buffer.limit() > Wal.LSN_OFFSET_MASK) {
                throw new IOException(this.file.getAbsolutePath() + " full");
            }

            final int offset = (int)pos;
            int p = buffer.position();
            IoUtils.writeInt(offset, buffer.array(), p);
            buffer.position(p += 4);
            IoUtils.writeInt(checksum, buffer.array(), p);
            buffer.position(p += 4);
            buffer.flip();

            final int n = p / BLOCK_SIZE, oldlim = buffer.limit();
            int lim = BLOCK_SIZE;
            for (i = 0; i < n; ++i) {
                buffer.limit(lim);
                for (; buffer.hasRemaining();) {
                    this.chan.write(buffer);
                }
                buffer.position(lim);
                lim += BLOCK_SIZE;
                buffer.limit(oldlim);
            }

            final int rem = p % BLOCK_SIZE;
            if (rem > 0) {
                for (; buffer.hasRemaining();) {
                    this.chan.write(buffer);
                }
            }
        }

        return true;
    }

    public void recovery() throws IOException {
        final long size = this.size();
        if (size < 8L) {
            this.chan.truncate(0L);
            return;
        }

        // Check integrity
        int offset;
        // 1. First back forward
        {
            final byte[] buf = new byte[8];
            final int p = (int)(size - buf.length);
            getBytes(p, buf);
            offset = IoUtils.readInt(buf);
            try {
                Wal wal = get(offset);
                IoUtils.info("walog last lsn 0x%x in '%s'", wal.getLsn(), this.file);
                this.chan.position(size);
                return;
            } catch (CorruptedException |EOFException e) {
                IoUtils.error("walog exit abnormally, recovery ...", e);
                offset = 0;
            }
        }
        // 2. Otherwise forward
        SimpleWal wal = null;
        try {
            for (; offset < size; ) {
                wal = (SimpleWal)get(offset);
                offset += (wal.getHeadSize() + wal.getData().length + 4);
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

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close()  {
        IoUtils.close(this.blockCache);
        IoUtils.close(this.chan);
        IoUtils.close(this.raf);

        this.open = false;
    }

}
