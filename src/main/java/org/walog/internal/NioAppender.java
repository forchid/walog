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

import org.walog.Wal;
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

import static java.util.concurrent.TimeUnit.*;

import static java.lang.Integer.getInteger;

/** The appender that actually execute wal "append" operation
 * for concurrency control.
 *
 * @author little-pan
 * @since 2020-01-04
 */
class NioAppender extends Thread implements AutoCloseable {

    static int QUEUE_SIZE = Integer.getInteger("org.walog.append.queueSize", 2048);
    static int BATCH_SIZE = Integer.getInteger("org.walog.append.batchSize", 1024);
    static int LOCK_TIMEOUT = getInteger("org.walog.append.lockTimeout", 50000);
    static int APPEND_TIMEOUT = getInteger("org.walog.append.timeout", LOCK_TIMEOUT);
    // Appender ID generator
    static final AtomicLong ID_GEN = new AtomicLong();

    // Basic states
    private volatile boolean open = true;

    // Batch properties
    protected final BlockingQueue<AppendItem<?>> appendQueue;
    protected final List<AppendPayloadItem> batchItems;
    protected final int batchSize;
    protected final NioWaler waler;

    // Basic resources
    private NioWalFile appendFile;
    private RandomAccessFile appendLockFile;
    private FileChannel appendLockChan;
    private volatile FileLock appendLock;

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

        this.appendQueue = new ArrayBlockingQueue<>(queueSize);
        this.batchItems  = new ArrayList<>(batchSize);
        this.batchSize   = batchSize;
        this.waler = waler;
    }

    public <V> V append(final AppendItem<V> item) throws IOException {
        try {
            if (!isOpen()) {
                throw new IOException("wal appender closed");
            }

            // Try to append into queue
            if (!this.appendQueue.offer(item, APPEND_TIMEOUT, MILLISECONDS)) {
                return null;
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }

        return (item.get());
    }

    @Override
    public void run() {
        AppendItem<?> item = null;
        boolean failed = true;
        try {
            for (; isOpen(); ) {
                int i = 0;
                // Batch prepare
                do {
                    if (item != null && item.tryRun()) {
                        switch (item.tag) {
                            case AppendItem.TAG_PAYLOAD:
                                this.batchItems.add((AppendPayloadItem)item);
                                item = null;
                                break;
                            case AppendItem.TAG_SYNC:
                                sync(item);
                                break;
                            case AppendItem.TAG_PURGE:
                                purgeTo((AppendPurgeToItem)item);
                                break;
                            case AppendItem.TAG_CLEAR:
                                clear(item);
                                break;
                            default:
                                String message = "Unsupported append item tag: " + item.tag;
                                item.setResult(new IllegalArgumentException(message));
                                break;
                        }
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

                // Try wait more
                try {
                    item = this.appendQueue.poll(1000, MILLISECONDS);
                } catch (final InterruptedException e) {
                    // Note: shouldn't interrupt this appender thread
                    // for NIO channel issue!
                }
            }
            cancelQueuedItems();
            failed = false;
        } catch (final Throwable cause) {
            setCauses(cause);
            if (item != null) {
                item.setResult(cause);
            }
        } finally {
            if (failed) {
                close();
            }
        }
    }

    protected void sync(AppendItem<?> item) throws IOException {
        doAppend();
        recovery();

        this.appendFile.sync();
        item.setResult(AppendItem.DUMMY_VALUE);
    }

    protected void purgeTo(AppendPurgeToItem item) throws IOException {
        doAppend();
        recovery();

        final File dir = this.waler.getDirectory();
        final File[] files = WalFileUtils.listFiles(dir, true);
        for (final File file: files) {
            if (file.getName().compareTo(item.walFile) < 0) {
                if (!file.delete()) {
                    item.setResult(new IOException("Can't purge wal file: " + file));
                    return;
                }
                continue;
            }
            break;
        }
        item.setResult(AppendItem.DUMMY_VALUE);
    }

    protected void clear(AppendItem<?> item) throws IOException {
        doAppend();
        recovery();

        // Prepare
        // - Close old append file
        final long nextFileLsn = WalFileUtils.nextFileLsn(this.appendFile.lsn);
        final File nextFile = this.waler.newFile(WalFileUtils.filename(nextFileLsn));
        IoUtils.close(this.appendFile);

        // - Create a new append file
        final File dir = this.waler.getDirectory();
        final File[] files = WalFileUtils.listFiles(dir, true);
        this.appendFile = new NioWalFile(nextFile);

        // Remove all previous files(include old append file)
        for (final File file: files) {
            if (!file.delete()) {
                item.setResult(new IOException("Can't delete wal file: " + nextFile));
            }
        }
        item.setResult(AppendItem.DUMMY_VALUE);
    }

    protected void doAppend() throws IOException {
        if (this.batchItems.size() == 0) {
            return;
        }

        recovery();
        if (this.appendFile.size() >= WalFileUtils.ROLL_SIZE) {
            rollFile();
        }

        final List<AppendPayloadItem> items = this.batchItems;
        final List<Wal> results = this.appendFile.append(items);
        final int n = items.size();
        for (int i = 0; i < n; ++i) {
            AppendPayloadItem item = items.get(i);
            item.setResult(results.get(i));
        }
        items.clear();
    }

    protected void recovery() throws IOException {
        if (this.appendLock != null && this.appendLock.isValid()) {
            return;
        }

        if (this.appendLockFile == null) {
            File lockFile = this.waler.newFile(".append.lock");
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
        final File dir = this.waler.getDirectory();
        boolean failed = true;
        try {
            File lastFile = WalFileUtils.lastFile(dir);
            if (lastFile == null) {
                String name = WalFileUtils.filename(0L);
                lastFile = new File(dir, name);
            }
            this.appendFile = new NioWalFile(lastFile);
            this.appendFile.recovery();
            this.appendLock = appendLock;
            failed = false;
        } finally {
            if (failed) {
                IoUtils.close(appendLock);
                IoUtils.close(this.appendFile);
                this.appendLock = null;
                this.appendFile = null;
            }
        }
    }

    protected FileLock appendLock() throws IOException {
        final int lockTimeout = LOCK_TIMEOUT;
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

    protected void rollFile() throws IOException {
        final long last = this.appendFile.getLsn();
        final long lsn = WalFileUtils.nextFileLsn(last);
        if (lsn < 0L) {
            throw new IOException("lsn full");
        }
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
        this.open = false;

        IoUtils.close(this.appendFile);
        IoUtils.close(this.appendLock);
        IoUtils.close(this.appendLockChan);
        IoUtils.close(this.appendLockFile);
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

}
