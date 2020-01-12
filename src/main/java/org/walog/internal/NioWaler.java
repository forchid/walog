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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

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
    public SimpleWal first() throws IOException {
        ensureOpen();

        final NioWalFile walFile = getFirstWalFile();
        if (walFile == null) {
            return null;
        }

        try {
            return walFile.get(0);
        } catch (EOFException e) {
            // No more or partial wal
            return null;
        }
    }

    @Override
    public Wal first(long timeout) throws IOException {
        Wal wal = first();
        if (wal != null || timeout < 0L) {
            return wal;
        }

        WatchService watchService = regWatchService();
        try {
            for (;;) {
                wal = first();
                if (wal != null) {
                    return wal;
                }
                final WatchKey watchKey;
                if (timeout == 0L) {
                    watchKey = watchPoll(watchService, timeout);
                } else {
                    long cur = System.currentTimeMillis();
                    watchKey = watchPoll(watchService, timeout);
                    timeout -= System.currentTimeMillis() - cur;
                    if (timeout <= 0L) {
                        return null;
                    }
                }
                if (watchKey != null) {
                    watchKey.pollEvents();
                    watchKey.reset();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } finally {
            IoUtils.close(watchService);
        }
    }

    protected static WatchKey watchPoll(WatchService watchService, long timeout)
            throws InterruptedException {
        if (timeout == 0L) {
            return watchService.take();
        } else {
            return watchService.poll(timeout, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public SimpleWal get(long lsn) throws IOException, IllegalArgumentException {
        checkLsn(lsn);
        ensureOpen();

        // WAl lookup basic algorithm:
        // 1) Find the log living in which wal file
        // 2) Skip to the offset in file, then read it
        NioWalFile walFile = getWalFile(lsn);
        if (walFile == null) {
            return null;
        }

        try {
            int offset = WalFileUtils.fileOffset(lsn);
            return walFile.get(offset);
        } catch (EOFException e) {
            // No more wal or partial wal
            return null;
        }
    }

    static void checkLsn(long lsn) throws IllegalArgumentException {
        if (lsn < 0L) {
            throw new IllegalArgumentException("lsn must be bigger than or equals 0: " + lsn);
        }
    }

    @Override
    public SimpleWal next(Wal wal) throws IOException, IllegalArgumentException {
        return next(wal, -1L);
    }

    @Override
    public SimpleWal next(final Wal wal, long timeout) throws IOException, IllegalArgumentException {
        SimpleWal w;

        if (wal instanceof SimpleWal) {
            w = (SimpleWal)wal;
            long nextLsn = w.nextLsn();
            w = get(nextLsn);
            if (w != null) {
                return w;
            }
            NioWalFile walFile = getWalFile(nextLsn);
            if (walFile == null) {
                return null;
            }

            WatchService watchService = null;
            try {
                final File file = walFile.getFile();
                final String filename = file.getName();
                for (;;) {
                    File last = WalFileUtils.lastFile(this.dir, filename);
                    if (!file.equals(last)) {
                        nextLsn = WalFileUtils.nextFileLsn(wal.getLsn());
                        w = get(nextLsn);
                    }
                    if (w != null || timeout < 0L) {
                        return w;
                    }

                    // wait logical
                    if (watchService == null) {
                        watchService = regWatchService();
                        // Retry get after register watcher
                        w = get(nextLsn);
                        if (w != null) {
                            return w;
                        }
                    }
                    final WatchKey watchKey;
                    if (timeout == 0L) {
                        watchKey = watchPoll(watchService, timeout);
                    } else {
                        long cur = System.currentTimeMillis();
                        watchKey = watchPoll(watchService, timeout);
                        timeout -= System.currentTimeMillis() - cur;
                        if (timeout <= 0L) {
                            return null;
                        }
                    }
                    if (watchKey != null) {
                        watchKey.pollEvents();
                        watchKey.reset();
                    }

                    w = get(nextLsn);
                    if (w != null) {
                        return w;
                    }
                } // for
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            } finally {
                IoUtils.close(watchService);
            }
        } else {
            w = get(wal.getLsn());
            if (w == null) {
                return null;
            }
            return next(w, timeout);
        }
    }

    protected WatchService regWatchService() throws IOException {
        WatchService watchService = null;
        boolean failed = true;
        try {
            watchService = FileSystems.getDefault().newWatchService();
            Path dirPath = Paths.get(this.dir.getAbsolutePath());
            dirPath.register(watchService,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
            failed = false;
            return watchService;
        } finally {
            if (failed) {
                IoUtils.close(watchService);
            }
        }
    }

    @Override
    public Iterator<Wal> iterator() {
        return new NioWalIterator(this);
    }

    @Override
    public Iterator<Wal> iterator(long lsn) throws IllegalArgumentException {
        return new NioWalIterator(this, lsn);
    }

    protected NioWalFile getFirstWalFile() throws IOException {
        final File file = WalFileUtils.firstFile(this.dir);
        if (file == null) {
            return null;
        }

        long fileLsn = WalFileUtils.lsn(file.getName());
        return getWalFile(fileLsn);
    }

    /** Acquire the specified lsn wal file.
     *
     * @param lsn wal serial number, or file lsn
     * @return the wal file, or null if the file not exits
     * @throws IOException if IO error
     * @throws IllegalArgumentException if the arg lsn is less than 0
     */
    protected NioWalFile getWalFile(long lsn) throws IOException, IllegalArgumentException {
        checkLsn(lsn);
        long fileLsn = WalFileUtils.fileLsn(lsn);

        NioWalFile walFile = this.walCache.get(fileLsn);
        if (walFile != null) {
            return walFile;
        }

        synchronized (this.walCache) {
            walFile = this.walCache.get(fileLsn);
            if (walFile != null) {
                return walFile;
            }
            String filename = WalFileUtils.filename(fileLsn);
            File file = new File(this.dir, filename);
            if (!file.isFile()) {
                return null;
            }
            walFile = new NioWalFile(file);
            this.walCache.put(fileLsn, walFile);
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
        this.open = false;

        IoUtils.close(this.walCache);
        synchronized (this.appenderInitLock) {
            IoUtils.close(this.appender);
            this.appender = null;
        }
    }

}
