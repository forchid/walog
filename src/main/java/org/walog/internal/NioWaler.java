/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 little-pan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.walog.internal;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.walog.Wal;
import org.walog.Waler;
import org.walog.util.IoUtils;
import org.walog.util.LruCache;
import org.walog.util.WalFileUtils;

/**
 * @author little-pan
 * @since 2019-12-22
 *
 */
public class NioWaler implements Waler {

    private volatile boolean open;

    protected final File dir;
    // file lsn -> wal file
    protected final LruCache<Long, NioWalFile> walCache;
    private final Object appenderInitLock = new Object();
    private volatile NioAppender appender;

    /** Create a WAL logger under the specified directory
     * 
     * @param dir the logger directory
     */
    public NioWaler(File dir) {
        this.dir = dir;
        this.walCache = new LruCache<>(WalFileUtils.CACHE_SIZE);
    }
    
    @Override
    public void open() throws IOException {
        if (isOpen()) {
            return;
        }
        
        final File dir = this.dir;
        if (!dir.isDirectory() && !dir.mkdir()) {
            throw new IOException("Can't create walog directory: " + dir);
        }
        this.open = true;
    }

    protected File getDirectory() {
        return this.dir;
    }

    protected File newFile(String filename) {
        return new File(this.dir, filename);
    }
    
    @Override
    public Wal append(byte[] payload) throws IOException {
        return append(payload, true);
    }

    @Override
    public Wal append(byte[] payload, int offset, int length) throws IOException {
        return append(Arrays.copyOfRange(payload, offset, offset + length), false);
    }

    @Override
    public Wal append(String payload) throws IOException {
        return append(payload.getBytes(Wal.CHARSET), false);
    }

    protected Wal append(byte[] payload, boolean copy) throws IOException {
        ensureOpen();
        if (copy) {
            payload = Arrays.copyOf(payload, payload.length);
        }
        final NioAppender appender = getAppender();
        final AppendPayloadItem item = new AppendPayloadItem(appender, payload);
        return appender.append(item);
    }

    protected NioAppender getAppender() {
        final NioAppender appender = this.appender;
        if (appender == null) {
            synchronized (this.appenderInitLock) {
                if (this.appender == null) {
                    this.appender = new NioAppender(this);
                    boolean failed = true;
                    try {
                        this.appender.start();
                        failed = false;
                    } finally {
                        if(failed) {
                            this.appender = null;
                        }
                    }
                }
                return this.appender;
            }
        }

        return appender;
    }

    @Override
    public Wal first() throws IOException {
        ensureOpen();

        final NioWalFile walFile = getFirstWalFile();
        if (walFile == null) {
            return null;
        }

        return walFile.get(0);
    }

    @Override
    public Wal get(final long lsn) throws IOException {
        ensureOpen();

        // WAl lookup basic algorithm:
        // 1) Find the log living in which wal file
        // 2) Skip to the offset in file, then read it
        if (lsn < 0L) {
            throw new IllegalArgumentException("lsn must bigger than or equals 0");
        }

        final NioWalFile walFile = getWalFile(lsn);
        if (walFile == null) {
            return null;
        }

        final int offset = WalFileUtils.fileOffset(lsn);
        return walFile.get(offset);
    }

    @Override
    public Iterator<Wal> iterator(long lsn) {
        return new NioWalIterator(this, lsn);
    }

    protected NioWalFile getFirstWalFile() throws IOException {
        final File file = WalFileUtils.firstFile(this.dir);
        if (file == null) {
            return null;
        }

        return getWalFile(WalFileUtils.lsn(file.getName()));
    }

    /** Acquire the specified lsn wal file.
     *
     * @param lsn wal serial number
     * @return the wal file, or null if the file not exits
     * @throws IOException if IO error
     */
    protected NioWalFile getWalFile(final long lsn) throws IOException {
        if (lsn < 0L) {
            throw new IllegalArgumentException("lsn must bigger than or equals 0");
        }

        NioWalFile walFile = this.walCache.get(lsn);
        if (walFile != null) {
            return walFile;
        }

        synchronized (this.walCache) {
            walFile = this.walCache.get(lsn);
            if (walFile != null) {
                return walFile;
            }

            File file = new File(this.dir, WalFileUtils.filename(lsn));
            if (!file.isFile()) {
                return null;
            }
            walFile = new NioWalFile(file);
            this.walCache.put(lsn, walFile);
        }

        return walFile;
    }

    @Override
    public boolean purgeTo(String filename) throws IOException {
        final AppendPurgeToItem item;
        ensureOpen();

        final NioAppender appender = getAppender();
        item = new AppendPurgeToItem(appender, filename);
        return appender.append(item);
    }

    @Override
    public boolean purgeTo(long fileLsn) throws IOException {
        return purgeTo(WalFileUtils.filename(fileLsn));
    }

    @Override
    public boolean clear() throws IOException {
        final AppendItem<Boolean> item;
        ensureOpen();

        final NioAppender appender = getAppender();
        item = new AppendItem<>(AppendItem.TAG_CLEAR, appender);
        return appender.append(item);
    }

    @Override
    public void sync() throws IOException {
        final AppendItem<Object> item;
        ensureOpen();

        final NioAppender appender = getAppender();
        item = new AppendItem<>(AppendItem.TAG_SYNC, appender);
        appender.append(item);
    }

    protected void ensureOpen() throws IOException {
        if (!isOpen()) throw new IOException("waler closed");
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() {
        if (!isOpen()) {
            return;
        }

        IoUtils.close(this.walCache);
        synchronized (this.appenderInitLock) {
            IoUtils.close(this.appender);
            this.appender = null;
        }
        this.open = false;
    }

}
