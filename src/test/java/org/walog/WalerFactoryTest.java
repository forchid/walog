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
import java.io.IOException;
import java.util.Iterator;
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
    protected void doTest() throws IOException {
        final File dirFile = getDir();
        // 10 million:
        // 2020-01-02 sync append mode:  1   thread  append 35758ms, iterate 9353ms
        // 2020-01-04 sync append mode:  10  threads append 84455ms, iterate 8826ms
        // 2020-01-04 async append mode: 1   thread  append 135522ms, iterate 8701ms
        // 2020-01-04 async append mode: 10  threads append 39339ms, iterate 8713ms
        // 2020-01-04 async append mode: 200 threads append 36877ms, iterate 9007ms
        // 2020-01-04 async append mode: 250 threads append 38148ms, iterate 8686ms
        final int n = 10_000_000, c = 10, step = n / c;
        
        final Waler walera = WalerFactory.open(dirFile);
        long startTime = System.currentTimeMillis();
        final Task<?>[] workers = new Task[c];
        for (int i = 0, m = 0; i < c; ++i) {
            final int s = m + step, e = s + step;
            m = e;
            final Task<Void> t = newTask(new Callable<Void>() {
                @Override
                public Void call() throws IOException {
                    final Random random = new Random();
                    for (int i = s; i < e; ++i) {
                        final int a = random.nextInt(i), b = random.nextInt(i);
                        final long cur = System.currentTimeMillis();
                        String data = cur+":"+a + "," + b + "," + (a + b);
                        Wal wal = walera.append(data.getBytes());
                        asserts( wal != null, "append wal failed");
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
        IoUtils.info("Append %d items, time %dms", n, (endTime - startTime));
        //sleep(5000);
        walera.close();

        final Waler walerb = WalerFactory.open(dirFile.getAbsolutePath());
        // Check-1
        Wal wal = walerb.first();
        checkWal(wal);
        // Check-2
        wal = walerb.get(wal.getLsn());
        checkWal(wal);
        // Check-3
        Iterator<Wal> itr = walerb.iterator(wal.getLsn());
        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            if(!itr.hasNext()) {
                throw new RuntimeException("Data lost at i " + i);
            }
            wal = itr.next();
            checkWal(wal);
        }
        if (itr.hasNext()) {
            throw new RuntimeException("Data too many");
        }
        endTime = System.currentTimeMillis();
        IoUtils.info("Iterate %d items, time %dms", n, (endTime - startTime));

        final File lastFile = WalFileUtils.lastFile(dirFile);
        if (lastFile == null) {
            throw new RuntimeException("Last wal file lost");
        }
        walerb.purgeTo(wal.getLsn());
        walerb.clear();

        walerb.close();
    }

    protected void checkWal(Wal wal) {
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
