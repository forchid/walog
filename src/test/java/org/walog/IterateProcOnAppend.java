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

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class IterateProcOnAppend {

    public static void main(String[] args) throws Exception {
        Thread.currentThread().setName("iterate-proc");

        final int argc = args.length;
        Integer appendItems = null;
        String dataDir = null;

        for (int i = 0; i < argc; ++i) {
            String arg = args[i];
            if ("--append-items".equals(arg)) {
                appendItems = Integer.decode(args[++i]);
            } else if ("--data-dir".equals(arg)) {
                dataDir = args[++i];
            }
        }
        if (appendItems == null) {
            IoUtils.error("No arg '--append-items' specified");
            System.exit(1);
        }
        if (dataDir == null) {
            IoUtils.error("No arg '--data-dir' specified");
            System.exit(1);
        }

        iterate(appendItems, dataDir);
    }

    static void iterate(int appendItems, String dataDir) throws IOException {
        IoUtils.info("Open in data dir: %s", dataDir);
        Waler walerI = WalerFactory.open(dataDir);
        long lsn = 0;
        boolean once = false;
        int n = appendItems + 1;
        Random rand = new Random();
        Iterator<Wal> itr = walerI.iterator(lsn);
        for (int i = 0; i < n; ++i) {
            for (; !itr.hasNext();) {
                IoUtils.debug("wait appender at i %d", i);
                Test.sleep(rand.nextInt(100));

                IoUtils.debug("re-iterate at i %d", i);
                itr = walerI.iterator(lsn);
                // Skip last item
                if(once && itr.hasNext()) itr.next();

                if (i >= appendItems) {
                    IoUtils.info("complete");
                    return;
                }
            }
            Wal wal = itr.next();
            lsn = wal.getLsn();
            once = true;
            String data = wal.toString();
            String[] parts = data.split("=");
            Test.asserts(parts.length == 2);
            Test.asserts(Integer.parseInt(parts[1]) == i, "i = " +i);
        }
        IoUtils.info("complete");

        walerI.close();
    }

}
