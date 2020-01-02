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
        // 2020-01-02 Append 35758ms, iterate 9353ms
        final int n = 10_000_000;
        
        Waler waler = WalerFactory.open(dirFile);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            if (!waler.append(data.getBytes())) {
                throw new RuntimeException("append wal failed");
            }
        }
        long endTime = System.currentTimeMillis();
        IoUtils.info("Append %d items, time %dms", n, (endTime - startTime));
        waler.close();

        waler = WalerFactory.open(dirFile.getAbsolutePath());
        // Check-1
        Wal wal = waler.first();
        String result = new String(wal.getData());
        if (!data.equals(result)){
            throw new RuntimeException("Data corrupted");
        }
        // Check-2
        wal = waler.get(wal.getLsn());
        result = new String(wal.getData());
        if (!data.equals(result)){
            throw new RuntimeException("Data corrupted");
        }
        // Check-3
        Iterator<Wal> itr = waler.iterator(0);
        startTime = System.currentTimeMillis();
        for (int i = 0; i < n; ++i) {
            if(!itr.hasNext()) {
                throw new RuntimeException("Data lost");
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

        waler.close();
    }

}
