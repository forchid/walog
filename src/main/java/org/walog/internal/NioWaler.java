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
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
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

    private NioWalFile appendFile;
    
    private RandomAccessFile appendLockFile;
    private FileChannel appendLockChan;
    private volatile FileLock appendLock;

    /** Create a WAL logger under the specified directory
     * 
     * @param dir the logger directory
     */
    public NioWaler(File dir) {
        this.dir = dir;
        this.walCache = new LruCache<>();
    }
    
    @Override
    public boolean open() throws IOException {
        if (isOpen()) {
            return true;
        }
        
        final File dir = this.dir;
        if (!dir.isDirectory() && !dir.mkdir()) {
            throw new IOException("Can't create walog directory: " + dir);
        }

        return true;
    }
    
    @Override
    public boolean append(byte[] payload) throws IOException {
        if (!recovery()) {
            return false;
        }

        synchronized (this) {
            if (this.appendFile.size() >= FILE_ROLL_SIZE) {
                rollFile();
            }
        }
        return this.appendFile.append(payload);
    }

    protected void rollFile() throws IOException {
        final long last = this.appendFile.getLsn();
        final long lsn = WalFileUtils.nextFileLsn(last);
        if (lsn < 0L) {
            throw new IOException("lsn full");
        }
        IoUtils.close(this.appendFile);

        IoUtils.debug("roll wal file: lsn 0x%x -> 0x%x", last, lsn);
        final String name = WalFileUtils.filename(lsn);
        final File lastFile = new File(this.dir, name);
        this.appendFile = new NioWalFile(lastFile, BLOCK_CACHE_SIZE);
        if (this.appendFile.size() != 0L) {
            throw new IllegalStateException(lastFile + " not a empty file");
        }
        IoUtils.debug("roll wal file to '%s' in '%s'", name, this.dir);
    }

    protected boolean recovery() throws IOException {
        if (this.appendLock != null && this.appendLock.isValid()) {
            return true;
        }

        synchronized(this) {
            if (this.appendLock != null && this.appendLock.isValid()) {
                return true;
            }

            if (this.appendLockFile == null) {
                File lockFile = new File(this.dir, ".append.lock");
                this.appendLockFile = new RandomAccessFile(lockFile, "rw");
                boolean failed = true;
                try {
                    this.appendLockChan = this.appendLockFile.getChannel();
                    failed = false;
                } finally {
                    if (failed) {
                        IoUtils.close(this.appendLockFile);
                    }
                }
            }

            final FileLock appendLock = appendLock();
            final File dir = this.dir;
            boolean failed = true;
            try {
                File lastFile = WalFileUtils.lastFile(dir);
                if (lastFile == null) {
                    String name = WalFileUtils.filename(0L);
                    lastFile = new File(dir, name);
                }
                this.appendFile = new NioWalFile(lastFile, BLOCK_CACHE_SIZE);
                this.appendFile.recovery();
                this.appendLock = appendLock;
                failed = false;
                return true;
            } finally {
                if (failed) {
                    IoUtils.close(appendLock);
                    IoUtils.close(this.appendFile);
                    this.appendLock = null;
                    this.appendFile = null;
                }
            }
        }
    }

    protected FileLock appendLock() throws IOException {
        final int lockTimeout = APPEND_LOCK_TIMEOUT;
        if (lockTimeout <= 0) {
            for (;;) {
                final FileLock lock = this.appendLockChan.lock();
                if (lock != null) {
                    return lock;
                }
            }
        }

        final long deadline = System.currentTimeMillis() + lockTimeout;
        try {
            for (;;) {
                final FileLock lock = this.appendLockChan.tryLock();
                if (lock != null) {
                    return lock;
                }

                Thread.sleep(10L);
                if (System.currentTimeMillis() > deadline) {
                    return null;
                }
            }
        } catch (InterruptedException e) {
            return null;
        }
    }

    @Override
    public Wal first() throws IOException {
        final NioWalFile walFile = getFirstWalFile();
        if (walFile == null) {
            return null;
        }

        return walFile.get(0);
    }

    @Override
    public Wal get(final long lsn) throws IOException {
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
            walFile = new NioWalFile(file, BLOCK_CACHE_SIZE);
            this.walCache.put(lsn, walFile);
        }

        return walFile;
    }

    @Override
    public void purgeTo(String walFile) throws IOException {
        // TODO Auto-generated method stub
    }

    @Override
    public boolean clear() throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean sync() throws IOException {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() {
        IoUtils.close(this.walCache);
        IoUtils.close(this.appendFile);
        
        IoUtils.close(this.appendLockChan);
        IoUtils.close(this.appendLockFile);
        this.open = false;
    }

}
