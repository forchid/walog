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

import org.walog.util.IoUtils;
import org.walog.util.WalFileUtils;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static java.util.concurrent.TimeUnit.*;

import static java.lang.Integer.getInteger;

/** The appender that actually execute wal "append" operation
 * for concurrency control.
 *
 * @author little-pan
 * @since 2020-01-04
 */
class NioAppender extends Thread implements AutoCloseable {

    static int QUEUE_SIZE     = getInteger("org.walog.append.queueSize", 128);
    static int BATCH_SIZE     = getInteger("org.walog.append.batchSize", 128);
    static int LOCK_TIMEOUT   = getInteger("org.walog.append.lockTimeout", 50000);
    static int APPEND_TIMEOUT = getInteger("org.walog.append.timeout", LOCK_TIMEOUT);
    static int ASYNC_MODE     = getInteger("org.walog.append.asyncMode", 1);
    static int AUTO_FLUSH     = getInteger("org.walog.append.autoFlush", 1);
    static int FLUSH_PERIOD   = getInteger("org.walog.append.flushPeriod", 100);
    static int FLUSH_UNLOCK   = getInteger("org.walog.append.flushUnlock", 1);

    // Appender ID generator
    static final AtomicLong ID_GEN = new AtomicLong();

    // Basic states
    private volatile boolean open = true;
    protected boolean appended;
    protected long syncTime;
    protected final NioWaler waler;

    // Batch properties
    protected final BlockingQueue<AppendItem<?>> appendQueue;
    protected final List<AppendPayloadItem> batchItems;
    protected final int batchSize;

    // Basic resources
    protected final ReentrantLock appendLock;
    private NioWalFile appendFile;
    private RandomAccessFile lockFile;
    private FileChannel lockChan;
    private FileLock fileLock;

    public NioAppender(NioWaler waler) {
        this(waler, QUEUE_SIZE, BATCH_SIZE);
    }

    public NioAppender(NioWaler waler, int queueSize, int batchSize) {
        if (queueSize < 1) {
            throw new IllegalArgumentException("queueSize: " + queueSize);
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize: " + batchSize);
        }
        setDaemon(true);
        setName("walog-appender-" + ID_GEN.getAndIncrement());

        this.batchItems  = new ArrayList<>(batchSize);
        this.batchSize   = batchSize;
        this.waler = waler;

        if (isAsyncMode()) {
            this.appendQueue = new ArrayBlockingQueue<>(queueSize);
            this.appendLock  = null;
        } else {
            this.appendQueue = null;
            this.appendLock  = new ReentrantLock();
        }
    }

    public <V> V append(final AppendItem<V> item) throws IOException {
        try {
            ensureOpen();

            if (isAsyncMode()) {
                // Try to append into queue
                if (this.appendQueue.offer(item, APPEND_TIMEOUT, MILLISECONDS)) {
                    // Check again: appender may be closed before item enqueued
                    ensureOpen();
                    return (item.get());
                }
            } else {
                if (item.tryRun()) {
                    if (this.appendLock.tryLock(APPEND_TIMEOUT, MILLISECONDS)) {
                        try {
                            handle(item, true);
                            return (item.get());
                        } finally {
                            this.appendLock.unlock();
                        }
                    }
                } else {
                    throw new IllegalStateException("Can't append again");
                }
            }
        } catch (FileLockTimeoutException e) {
            // Acquire append file lock timeout
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Append timeout or interrupted
        return null;
    }

    protected void ensureOpen() throws IOException {
        if (!isOpen()) {
            throw new IOException("wal appender closed");
        }
    }

    @Override
    public void run() {
        if (!isAsyncMode()) {
            IoUtils.info("wal appender runes in sync mode");
            return;
        }
        IoUtils.info("wal appender runes in async mode");

        AppendItem<?> item = null;
        try {
            while (isOpen()) {
                try {
                    int i = 0;
                    // Batch prepare
                    do {
                        if (item != null && item.tryRun()) {
                            handle(item, false);
                            ++i;
                        }
                        item = null;
                        if (i >= this.batchSize) {
                            break;
                        }

                        // Try next item
                        item = this.appendQueue.poll();
                    } while (item != null);

                    // Do batch append
                    doAppend();
                    // Sync storage state
                    if (this.appended && isAutoFlush() && isFlushTime()) {
                        IoUtils.debug("Auto flush start");
                        recovery();
                        this.appendFile.sync();
                        this.appended = false;
                        this.syncTime = System.currentTimeMillis();
                        IoUtils.debug("Auto flush end");
                        if (isFlushUnlock()) {
                            releaseFileLock(this.fileLock);
                        }
                    }

                    // Try wait more
                    item = this.appendQueue.poll(1000L, MILLISECONDS);
                } catch (FileLockTimeoutException e) {
                    if (item != null) {
                        item.setResult(null);
                    }
                }
            } // loop

            cancelQueuedItems();
        } catch (final Throwable cause) {
            setCauses(cause);
            if (item != null) {
                item.setResult(cause);
            }
        } finally {
            close();
        }
    }

    protected boolean isFlushTime() {
        return (FLUSH_PERIOD <= 0 || System.currentTimeMillis()-this.syncTime >= FLUSH_PERIOD);
    }

    protected void handle(AppendItem<?> item, boolean syncAppend) throws IOException {
        boolean fatal = true;
        try {
            switch (item.tag) {
                case AppendItem.TAG_PAYLOAD:
                    this.batchItems.add((AppendPayloadItem) item);
                    if (syncAppend) {
                        doAppend();
                    }
                    break;
                case AppendItem.TAG_SYNC:
                    sync(item);
                    break;
                case AppendItem.TAG_PURGE:
                    purgeTo((AppendPurgeToItem) item);
                    break;
                case AppendItem.TAG_CLEAR:
                    clear(item);
                    break;
                default:
                    String message = "Unsupported append item tag: " + item.tag;
                    item.setResult(new IllegalArgumentException(message));
                    break;
            }
            fatal = false;
        } catch (FileLockTimeoutException e) {
            fatal = false;
            throw e;
        } finally {
            if (fatal) {
                close();
            }
        }
    }

    protected void sync(AppendItem<?> item) throws IOException {
        doAppend();
        recovery();

        this.appendFile.sync();
        this.appended = false;
        this.syncTime = System.currentTimeMillis();
        item.setResult(AppendItem.DUMMY_VALUE);
        if (FLUSH_UNLOCK == 1) {
            releaseFileLock(this.fileLock);
        }
    }

    protected void purgeTo(AppendPurgeToItem item) {
        File dir = this.waler.getDirectory();
        File[] files = WalFileUtils.listFilesTo(dir, true, item.filename);
        for (File file: files) {
            IoUtils.debug("Purge wal file '%s'", file);
            if (file.exists() && !file.delete()) {
                IoUtils.debug("Can't purge wal file '%s'", file);
                item.setResult(Boolean.FALSE);
                return;
            }
        }
        item.setResult(Boolean.TRUE);
    }

    protected void clear(AppendItem<?> item) throws IOException {
        doAppend();
        recovery();

        // Prepare
        // - Close old append file
        long nextFileLsn = WalFileUtils.nextFileLsn(this.appendFile.lsn);
        String nextFilename = WalFileUtils.filename(nextFileLsn);
        File nextFile = this.waler.newFile(nextFilename);
        IoUtils.close(this.appendFile);

        // - Create a new append file
        File dir = this.waler.getDirectory();
        File[] files = WalFileUtils.listFiles(dir, true);
        this.appendFile = new NioWalFile(nextFile);

        // Remove all previous files(include old append file)
        for (File file: files) {
            IoUtils.debug("Delete wal file '%s'", file);
            if (file.exists() && !file.delete()) {
                IoUtils.debug("Can't delete wal file '%s'", file);
                item.setResult(Boolean.FALSE);
                return;
            }
        }
        item.setResult(Boolean.TRUE);
    }

    protected void doAppend() throws IOException {
        if (this.batchItems.size() == 0) {
            return;
        }

        recovery();
        if (this.appendFile.size() >= WalFileUtils.ROLL_SIZE) {
            rollFile();
        }

        List<AppendPayloadItem> items = this.batchItems;
        this.appendFile.append(items);
        for (int i = 0, n = items.size(); i < n; ++i) {
            AppendPayloadItem item = items.get(i);
            item.setResult(item.wal);
        }
        items.clear();

        this.appended = true;
    }

    protected void recovery() throws IOException {
        if (this.fileLock != null && this.fileLock.isValid()) {
            return;
        }

        if (this.lockChan == null || !this.lockChan.isOpen()) {
            File lockFile = this.waler.newFile("append.lock");
            RandomAccessFile raf = new RandomAccessFile(lockFile, "rw");
            boolean failed = true;
            try {
                this.lockChan = raf.getChannel();
                this.lockFile = raf;
                failed = false;
            } finally {
                if (failed) {
                    IoUtils.close(raf);
                }
            }
        }

        FileLock appendFileLock = null;
        boolean failed = true;
        try {
            appendFileLock = acquireFileLock();
            if (appendFileLock == null) {
                throw new IllegalStateException("'append file lock' null");
            }
            final File dir = this.waler.getDirectory();
            File lastFile = WalFileUtils.lastFile(dir);
            if (lastFile == null) {
                String name = WalFileUtils.filename(0L);
                lastFile = new File(dir, name);
            }
            this.appendFile = new NioWalFile(lastFile);
            this.appendFile.recovery();
            this.fileLock = appendFileLock;
            failed = false;
        } finally {
            if (failed) {
                releaseFileLock(appendFileLock);
            }
        }
    }

    protected FileLock acquireFileLock() throws IOException {
        final int lockTimeout = LOCK_TIMEOUT;
        if (lockTimeout <= 0) {
            for (;;) {
                final FileLock lock = this.lockChan.lock();
                if (lock != null) {
                    return lock;
                }
            }
        }

        final long deadline = System.currentTimeMillis() + lockTimeout;
        for (;;) {
            final FileLock lock = this.lockChan.tryLock();
            if (lock != null) {
                return lock;
            }

            if (System.currentTimeMillis() > deadline) {
                throw new FileLockTimeoutException("Acquire append file lock timeout");
            }
        }
    }

    private void releaseFileLock(FileLock fileLock) {
        try {
            IoUtils.close(this.appendFile);
            this.appendFile = null;
        } finally {
            IoUtils.close(fileLock);
            this.fileLock = null;
        }
    }

    protected void rollFile() throws IOException {
        final long last = this.appendFile.getLsn();
        final long lsn = WalFileUtils.nextFileLsn(last);
        if (lsn < 0L) {
            throw new IOException("lsn full");
        }
        this.appendFile.sync();
        IoUtils.close(this.appendFile);

        IoUtils.debug("roll wal file: lsn 0x%x -> 0x%x", last, lsn);
        final String name = WalFileUtils.filename(lsn);
        final File dir = this.waler.getDirectory();
        final File lastFile = new File(dir, name);
        this.appendFile = new NioWalFile(lastFile);
        if (this.appendFile.size() != 0L) {
            throw new IllegalStateException(lastFile + " not a empty file");
        }
        IoUtils.debug("roll wal file to '%s' in '%s'", name, dir);
    }

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() {
        releaseFileLock(this.fileLock);

        IoUtils.close(this.lockChan);
        IoUtils.close(this.lockFile);
        this.lockChan = null;
        this.lockFile = null;

        this.open = false;
    }

    protected void cancelQueuedItems() {
        AppendItem<?> item;
        for (;;) {
            item = this.appendQueue.poll();
            if (item == null) {
                break;
            }
            item.cancel();
        }
    }

    protected void setCauses(final Throwable cause) {
        AppendItem<?> item;

        int n = this.batchItems.size();
        for (int i = 0; i < n; ++i) {
            item = this.batchItems.get(i);
            item.setResult(cause);
        }

        for (;;) {
            item = this.appendQueue.poll();
            if (item == null) {
                break;
            }
            item.setResult(cause);
        }
    }

    protected static boolean isAsyncMode() {
        return (ASYNC_MODE == 1);
    }

    protected static boolean isAutoFlush() {
        return (AUTO_FLUSH == 1);
    }

    protected static boolean isFlushUnlock() {
        return (FLUSH_UNLOCK == 1);
    }

}
