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
import org.walog.util.WalFileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * @author little-pan
 * @since 2019-12-23
 *
 */
public class WalerFactoryTest extends Test {

    protected static final String dir = "walerFactory";

    public static void main(String[] args) throws IOException {
        new WalerFactoryTest().test();
    }

    @Override
    protected void cleanup() {
        deleteDir(dir);
    }

    @Override
    protected void doTest() throws IOException {
        final File dirFile = getDir(dir);
        final String data = "Hello, walog!";
        // 10 million:
        // 2020-01-02 Direct append: 1 thread append 35758ms, iterate 9353ms
        // 2020-01-04 Background append: 1 thread 135522ms, iterate 8701ms
        // 2020-01-04 Background append: 10 threads 39339ms, iterate 8713ms
        // 2020-01-04 Background append: 200 threads 36877ms, iterate 9007ms
        // 2020-01-04 Background append: 250 threads 38148ms, iterate 8686ms
        final int n = 10_000_000, c = 10, step = n / c;
        
        final Waler walera = WalerFactory.open(dirFile);
        long startTime = System.currentTimeMillis();
        final Thread[] workers = new Thread[c];
        for (int i = 0, m = 0; i < c; ++i) {
            final int s = m + step, e = s + step;
            m = e;
            final Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (int i = s; i < e; ++i) {
                            if (walera.append(data.getBytes()) == null) {
                                throw new RuntimeException("append wal failed");
                            }
                            if (i % 10000 == 0) {
                                walera.sync();
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }, "worker-"+i);
            t.start();
            workers[i] = t;
        }
        for (final Thread t: workers) {
            try {
                t.join();
            } catch (InterruptedException e) {
                // Ignore
            }
        }
        long endTime = System.currentTimeMillis();
        IoUtils.info("Append %d items, time %dms", n, (endTime - startTime));
        walera.close();

        final Waler walerb = WalerFactory.open(dirFile.getAbsolutePath());
        // Check-1
        Wal wal = walerb.first();
        String result = new String(wal.getData());
        if (!data.equals(result)){
            throw new RuntimeException("Data corrupted");
        }
        // Check-2
        wal = walerb.get(wal.getLsn());
        result = new String(wal.getData());
        if (!data.equals(result)){
            throw new RuntimeException("Data corrupted");
        }
        // Check-3
        Iterator<Wal> itr = walerb.iterator(wal.getLsn());
        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            if(!itr.hasNext()) {
                throw new RuntimeException("Data lost at i " + i);
            }
            wal = itr.next();
            result = new String(wal.getData());
            if (!data.equals(result)){
                throw new RuntimeException("Data corrupted");
            }
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
        walerb.purgeTo(lastFile.getName());
        walerb.clear();

        walerb.close();
    }

}
