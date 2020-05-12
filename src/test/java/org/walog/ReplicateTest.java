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
        File testDir = getDir();
        File masterDir = getDir(testDir, "master");
        File slaveDir = getDir(testDir, "slave");

        InProcWalSlave slave = new InProcWalSlave(null);
        try {
            try (Waler slaveWaler = WalerFactory.open(slaveDir, slave);
                 Waler masterWaler = WalerFactory.open(masterDir)) {

                InProcWalMaster master = new InProcWalMaster(masterWaler, slave);
                slave.setMaster(master);

                master.onActive();
                slave.onActive();

                masterWaler.append("1");
                masterWaler.append("2");

                Wal wal = slaveWaler.first(0);
                asserts("1".equals(new String(wal.getData(), Wal.CHARSET)));
                wal = slaveWaler.next(wal, 0);
                asserts("2".equals(new String(wal.getData(), Wal.CHARSET)));
            }
        } finally {
            slave.onInactive();
        }
    }

}
