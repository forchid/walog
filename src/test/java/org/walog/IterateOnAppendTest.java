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

package org.walog;

import org.walog.util.IoUtils;
import org.walog.util.Task;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.Callable;

public class IterateOnAppendTest extends Test {

    public static void main(String[] args) {
        for (int i = 0; i < iterates; ++i)
            new IterateOnAppendTest(i).test();
    }

    public IterateOnAppendTest(int iterate) {
        super(iterate);
    }

    @Override
    protected void doTest() throws IOException {
        final File dir = getDir();
        final Waler waler = WalerFactory.open(dir);
        final int appendItems = 100000;

        Task<Void> appender = newTask(new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                int n = appendItems;
                for(int i = 0; i < n; ++i) {
                    waler.append(System.currentTimeMillis()+": i=" + i);
                }
                IoUtils.debug("complete");
                return null;
            }
        }, "appender");

        Task<Void> iterator = newTask(new Callable<Void>() {
            @Override
            public Void call() {
                long lsn = 0;
                boolean once = false;
                int n = appendItems + 1;
                Random rand = new Random();
                Iterator<Wal> itr = waler.iterator(lsn);
                for (int i = 0; i < n; ++i) {
                    for (; !itr.hasNext();) {
                        IoUtils.debug("wait appender at i %d", i);
                        sleep(rand.nextInt(100));

                        IoUtils.debug("re-iterate at i %d", i);
                        itr = waler.iterator(lsn);
                        // Skip last item
                        if(once && itr.hasNext()) itr.next();

                        if (i >= appendItems) {
                            IoUtils.debug("complete");
                            return null;
                        }
                    }
                    Wal wal = itr.next();
                    lsn = wal.getLsn();
                    once = true;
                    String data = wal.toString();
                    String[] parts = data.split("=");
                    asserts(parts.length == 2);
                    asserts(Integer.parseInt(parts[1]) == i, "i = " +i);
                }
                IoUtils.debug("complete");
                return null;
            }
        }, "iterator");

        iterator.start();
        appender.start();

        join(iterator);
        join(appender);
        iterator.check();
        appender.check();

        waler.close();
    }

}
