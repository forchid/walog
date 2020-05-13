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

import java.io.File;

public class ReplicateTest extends Test {

    public static void main(String[] args) {
        new ReplicateTest(iterates).test();
    }

    public ReplicateTest(int iterate) {
        super(iterate);
    }

    @Override
    protected void doTest() {
        int dataBaseLen = 36;
        int items = 1_000_000;
        inProcTest(dataBaseLen, items);

        cleanup();
        dataBaseLen = (4 << 10) - 30;
        items = 1_00_000;
        inProcTest(dataBaseLen, items);

        cleanup();
        dataBaseLen = 4 << 10;
        items = 1_00_000;
        inProcTest(dataBaseLen, items);
    }

    private void inProcTest(int dataBaseLen, int items) {
        File testDir = getDir();
        File masterDir = getDir(testDir, "master");
        File slaveDir = getDir(testDir, "slave-a");

        final String prefix;
        StringBuilder sb = new StringBuilder(dataBaseLen);
        for (int i = 0; i < dataBaseLen; ++i) {
            if (i % 2 == 0) {
                sb.append('A');
            } else {
                sb.append('B');
            }
        }
        prefix = sb.toString();

        // Test cases:
        // 1) Replicate from the start wal
        // 2) Replicate from specified wal
        int caseIt = 0, case2Items = 1000;
        Wal fromWal = null;
        while (caseIt < 2) {
            if (caseIt == 0) {
                IoUtils.info("Replicate from the start wal");
            } else {
                IoUtils.info("Replicate from the wal 0x%x", fromWal.getLsn());
            }

            InProcWalSlave slave = new InProcWalSlave(fromWal);
            try {
                try (Waler slaveWaler = WalerFactory.open(slaveDir, slave);
                     Waler masterWaler = WalerFactory.open(masterDir)) {

                    InProcWalMaster master = new InProcWalMaster(masterWaler, slave);
                    slave.setMaster(master);

                    master.onActive();
                    slave.onActive();

                    final int n;
                    int i;
                    if (caseIt == 0) {
                        i = 0;
                        n = items;
                    } else {
                        n = items + case2Items;
                        i = items;
                    }

                    // Master writes
                    for (; i < n; ++i) {
                        masterWaler.append(prefix + i);
                    }

                    // Slave reads
                    Wal wal;
                    if (caseIt == 0) {
                        i = 0;
                        wal = slaveWaler.first(0);
                    } else {
                        i = items;
                        assert fromWal != null;
                        wal = slaveWaler.next(fromWal, 0);
                    }
                    do {
                        String log  = prefix + i;
                        String data = new String(wal.getData(), Wal.CHARSET);
                        asserts(log.equals(data), "Replicated wal not matched");
                        if (++i >= n) {
                            break;
                        }
                        wal = slaveWaler.next(wal, 0);
                        fromWal = wal;
                    } while (true);

                    asserts(slave.bytesBehindMaster() == 0L);
                }

                ++caseIt;
            } finally {
                slave.onInactive();
            }
        }
    }

}
