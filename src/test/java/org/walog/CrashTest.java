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
import org.walog.util.Proc;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

public class CrashTest extends Test {

    public static void main(String[] args) {
        for (int i = 0; i < iterates; ++i) {
            new CrashTest(i).test();
        }
        completed = true;
    }

    protected CrashTest(int iterate) {
        super(iterate);
    }

    @Override
    protected void doTest() throws IOException {
        final File dir = getDir();
        final String mainClass = WalServerProc.class.getName();
        final String prefix = "Crash-test-Crash-test-Crash-test-";

        // Steps:
        // 1) Destroy append-process randomly
        // 2) Then check data integrity and duration
        // 3) Cleanup resources and retry next
        final int times = 10, randUp = 1000;
        final long timeout = 1000;
        final Random random = new Random();
        Proc child = null;
        String[] procArgs = {"--data-dir", dir + "", "--proto", "rmi"};
        try {
            for (int i = 0; i < times; ++i) {
                final int r = Math.max(1, random.nextInt(randUp));
                IoUtils.debug("random num: %s", r);
                IoUtils.info("Start wal server");
                child = newProc(mainClass, procArgs);
                final String curDir = System.getProperty("user.dir");
                child.setWorkDir(curDir);
                final String target = curDir+File.separator+"target";
                child.setProperties(new String[]{"-classpath",
                        target+File.separator+"classes" + File.pathSeparator + target+File.separator+"test-classes",
                        "-Dorg.walog.append.asyncMode=1",
                        //"-Dorg.walog.debug=true"
                });
                child.start();
                Waler waler = connect("walog:rmi://localhost/wal", timeout);
                // Append then kill
                for (int j = 0; j < r; ++j) {
                    waler.append(prefix + j);
                }
                child.kill();
                child.join();
                child = null;
                IoUtils.info("Kill wal server");

                // Check
                IoUtils.info("Start wal server");
                child = newProc(mainClass, procArgs);
                child.setWorkDir(curDir);
                child.setProperties(new String[]{"-classpath",
                        target+File.separator+"classes" + File.pathSeparator + target+File.separator+"test-classes",
                        "-Dorg.walog.append.asyncMode=1",
                        //"-Dorg.walog.debug=true"
                });
                child.start();
                waler = connect("walog:rmi://localhost/wal", timeout);
                Iterator<Wal> it = waler.iterator();
                for (int j = 0; j < r; ++j) {
                    asserts(it.hasNext());
                    String wal = it.next() + "";
                    asserts(wal.equals(prefix + j));
                }
                asserts(!it.hasNext());
                child.kill();
                child.join();
                child = null;
                IoUtils.info("Kill wal server");

                cleanup();
            }
        } catch (InterruptedException e) {
            throw new AssertionError("Interrupted", e);
        } finally {
            if (child != null) {
                child.kill();
                try {
                    child.join();
                } catch (InterruptedException e) {
                    // Ignore
                }
            }
        }
    }

    static Waler connect(String url, long timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout;
        while (true) {
            try {
                Waler waler = WalDriverManager.connect(url);
                IoUtils.info("Connect wal server ok: url '%s'", url);
                return waler;
            } catch (NetWalException e) {
                if (System.currentTimeMillis() - deadline > 0) throw e;
                Thread.yield();
            }
        }
    }

}
