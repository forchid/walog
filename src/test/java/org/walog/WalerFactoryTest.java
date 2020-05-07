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

package org.walog;

import org.walog.util.IoUtils;
import org.walog.util.Task;
import org.walog.util.WalFileUtils;

import java.io.File;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 * @author little-pan
 * @since 2019-12-23
 *
 */
public class WalerFactoryTest extends Test {

    public static void main(String[] args) {
        new WalerFactoryTest(0).test();
    }

    public WalerFactoryTest(int iterate) {
        super(iterate);
    }

    @Override
    protected void doTest() {
        // Test multi-thread read caches
        final String cacheSizeProp = "org.walog.file.cacheSize";
        final String oldCacheSize = System.getProperty(cacheSizeProp);
        System.setProperty(cacheSizeProp, "2");

        final File dirFile = getDir();
        // 10 million:
        // 2020-01-02 sync append mode:  1   thread  append 35758ms, iterate 9353ms
        // 2020-01-04 sync append mode:  10  threads append 84455ms, iterate 8826ms
        // 2020-01-04 async append mode: 1   thread  append 135522ms, iterate 8701ms
        // 2020-01-04 async append mode: 10  threads append 39339ms, iterate 8713ms
        // 2020-01-04 async append mode: 200 threads append 36877ms, iterate 9007ms
        // 2020-01-04 async append mode: 250 threads append 38148ms, iterate 8686ms
        int n = 10_000_000, c = 10, step = n / c;
        
        final Waler walera = WalerFactory.open(dirFile);
        long startTime = System.currentTimeMillis();
        Task<?>[] workers = new Task[c];
        for (int i = 0, m = 0; i < c; ++i) {
            final int s = m + step, e = s + step;
            m = e;
            final Task<Void> t = newTask(new Callable<Void>() {
                @Override
                public Void call() {
                    final Random random = new Random();
                    for (int i = s; i < e; ++i) {
                        final int a = random.nextInt(i), b = random.nextInt(i);
                        final long cur = System.currentTimeMillis();
                        String data = cur+":"+a + "," + b + "," + (a + b);
                        Wal wal = walera.append(data);
                        asserts( wal != null);
                        if (i % 10000 == 0) {
                            walera.sync();
                        }
                    }
                    return null;
                }
            }, "worker-"+i);
            t.start();
            workers[i] = t;
        }
        for (final Task<?> t: workers) {
            join(t);
            t.check();
        }
        long endTime = System.currentTimeMillis();
        IoUtils.info("Append %d items, time %dms, threads %d", n, (endTime - startTime), c);
        walera.close();

        final Waler walerb = WalerFactory.open(dirFile.getAbsolutePath());
        // Check first
        Wal wal = walerb.first();
        final Wal first = wal;
        checkWal(wal);
        // Check get(lsn)
        wal = walerb.get(first.getLsn());
        checkWal(wal);

        // Check by iterate(lsn) - multi-thread
        c = 20;
        workers = new Task[c];
        startTime = System.currentTimeMillis();
        for (int i = 0; i < c; ++i) {
            Task<Void> t = newTask(new Callable<Void>() {
                @Override
                public Void call() {
                    try(WalIterator itr = walerb.iterator(first.getLsn())) {
                        for (int i = 0; i < n; ++i) {
                            asserts(itr.hasNext(), "Data lost at i " + i);
                            Wal wal = itr.next();
                            checkWal(wal);
                        }
                        asserts(!itr.hasNext(), "Data too many");
                    }
                    return null;
                }
            });
            workers[i] = t;
            t.start();
        }
        for (final Task<?> t: workers) {
            join(t);
            t.check();
        }
        endTime = System.currentTimeMillis();
        IoUtils.info("Iterate-from-lsn %d items, time %dms, threads %d", n * c, (endTime - startTime), c);

        // Check by iterate(lsn, 0)
        WalIterator itr = walerb.iterator(first.getLsn(), 0);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            asserts(itr.hasNext(), "Data lost at i " + i);
            wal = itr.next();
            checkWal(wal);
        }
        itr.close();
        asserts (walerb.next(wal) == null, "Data too many");
        endTime = System.currentTimeMillis();
        IoUtils.info("Iterate-from-lsn-block %d items, time %dms", n, (endTime - startTime));

        // Check by iterate(lsn, timeout)
        itr = walerb.iterator(first.getLsn(), 1000);
        startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < n; ++i) {
                asserts(itr.hasNext(), "Data lost at i " + i);
                wal = itr.next();
                checkWal(wal);
            }
            asserts(!itr.hasNext(), "Data too many");
        } catch (TimeoutWalException e) {
            // pass
        } finally {
            itr.close();
        }
        endTime = System.currentTimeMillis();
        IoUtils.info("Iterate-from-lsn-timeout %d items, time %dms", n, (endTime - startTime));

        // Check by iterate()
        itr = walerb.iterator();
        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            asserts(itr.hasNext(), "Data lost at i " + i);
            wal = itr.next();
            checkWal(wal);
        }
        asserts (!itr.hasNext(), "Data too many");
        itr.close();
        endTime = System.currentTimeMillis();
        IoUtils.info("Iterate %d items, time %dms", n, (endTime - startTime));

        // Check by next(wal)
        startTime = System.currentTimeMillis();
        int i = 0;
        wal = walerb.first();
        do {
            ++i;
            checkWal(wal, i);
            wal = walerb.next(wal);
        } while (i < n);
        asserts(i == n && wal == null);
        endTime = System.currentTimeMillis();
        IoUtils.info("Next %d items, time %dms", n, (endTime - startTime));

        // Check by next(wal, timeout)
        startTime = System.currentTimeMillis();
        i = 0;
        Wal last;
        wal = walerb.first();
        do {
            ++i;
            checkWal(wal, i);
            last = wal;
            try {
                wal = walerb.next(wal, i < n ? 0 : 100L);
            } catch (TimeoutWalException e) {
                // pass
                wal = null;
            }
        } while (i < n);
        asserts(i == n && wal == null);
        endTime = System.currentTimeMillis();
        IoUtils.info("Next-timeout %d items, time %dms", n, (endTime - startTime));

        final File lastFile = WalFileUtils.lastFile(dirFile);
        if (lastFile == null) {
            throw new RuntimeException("Last wal file lost");
        }
        asserts(walerb.purgeTo(last.getLsn()));
        asserts(walerb.clear());

        walerb.close();
        if (oldCacheSize != null) {
            System.setProperty(cacheSizeProp, oldCacheSize);
        }
    }

    protected void checkWal(Wal wal) {
        checkWal(wal, 0);
    }

    protected void checkWal(Wal wal, int index) {
        asserts(wal != null, "index = " + index);
        // timestamp:a,b,sum
        String result = new String(wal.getData());
        //IoUtils.info("log data: %s", result);
        int i = result.indexOf(':');
        asserts (i != -1, "Data corrupted");

        result = result.substring(i + 1);
        String[] sa = result.split(",");
        asserts (sa.length == 3, "Data corrupted");

        final int a = Integer.parseInt(sa[0]), b = Integer.parseInt(sa[1]);
        final int sum = Integer.parseInt(sa[2]);
        asserts (a + b == sum, "Data corrupted");
    }

}
