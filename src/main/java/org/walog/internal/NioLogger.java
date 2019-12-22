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

import org.walog.Log;
import org.walog.Logger;
import org.walog.util.IoUtils;
import org.walog.util.LogFileUtils;

/**
 * @author little-pan
 * @since 2019-12-22
 *
 */
public class NioLogger implements Logger {
    
    protected final File dir;
    private RandomAccessFile appendFile;
    private FileChannel appendChan;
    
    private RandomAccessFile appendLockFile;
    private FileChannel appendLockChan;
    
    private volatile boolean open;

    /** Create a logger under the specified directory
     * 
     * @param dir the logger directory
     */
    public NioLogger(File dir) {
        this.dir = dir;
    }
    
    @Override
    public boolean open() throws IOException {
        boolean success;
        
        if (isOpen()) {
            return true;
        }
        
        final File dir = this.dir;
        if (!dir.isDirectory() && !dir.mkdir()) {
            throw new IOException("Can't create log directory: " + dir);
        }
        
        synchronized(this) {
            if (isOpen()) {
                return true;
            }
            File lockFile = new File(this.dir, "append.lock");
            this.appendLockFile = new RandomAccessFile(lockFile, "rw");
            
            boolean failed = true;
            try {
                this.appendLockChan = this.appendLockFile.getChannel();
                failed = false;
            } finally {
                if (failed) {
                    this.appendLockFile.close();
                }
            }
            
            success = recovery();
            this.open = true;
        }
        
        return success;
    }
    
    protected boolean recovery() throws IOException {
        final FileLock appendLock = appendLock();
        if (appendLock == null) {
            return false;
        }
        
        final File dir = this.dir;
        try {
            File lastFile = LogFileUtils.lastFile(dir);
            if (lastFile == null) {
                String name = LogFileUtils.filename(0L);
                lastFile = new File(dir, name);
            }
            this.appendFile = new RandomAccessFile(lastFile, "rw");
            this.appendChan = this.appendFile.getChannel();
            doRecovery();
        } finally {
            appendLock.release();
        }
        
        return true;
    }
    
    protected void doRecovery() throws IOException {
        if (this.appendChan.size() == 0L) {
            return;
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
    public Log append(byte[] payload) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Log next() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Log next(long lsn) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Log get() throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Log get(long lsn) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long purgeTo(long lsn) throws IOException {
        // TODO Auto-generated method stub
        return 0;
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
        IoUtils.close(this.appendChan);
        IoUtils.close(this.appendFile);
        
        IoUtils.close(this.appendLockChan);
        IoUtils.close(this.appendLockFile);
        this.open = false;
    }

}
