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
import java.util.Properties;

public class ReplicateTest extends Test {

    public static void main(String[] args) {
        for (int i = 0; i < iterates; ++i) {
            new ReplicateTest(i).test();
        }
        completed = true;
    }

    WalServer walServer;
    SlaveWaler slave;
    Waler master;

    public ReplicateTest(int iterate) {
        super(iterate);
    }

    @Override
    protected void cleanup() {
        IoUtils.close(this.slave);
        IoUtils.close(this.walServer);
        super.cleanup();
    }

    @Override
    protected void doTest() {
        int dataBaseLen = 36;
        int items = 1_000_000;

        inprocTest(dataBaseLen, items, false);
        cleanup();
        remoteTest(dataBaseLen, items, false);
        cleanup();
        inprocTest(dataBaseLen, items, false);

        dataBaseLen = (4 << 10) - 30;
        items = 1_00_000;
        cleanup();
        inprocTest(dataBaseLen, items, true);
        cleanup();
        remoteTest(dataBaseLen, items, true);

        dataBaseLen = 4 << 10;
        items = 1_00_000;
        cleanup();
        inprocTest(dataBaseLen, items, true);
        cleanup();
        remoteTest(dataBaseLen, items, true);

        // Auth test
        dataBaseLen = 1024;
        items = 1_000;
        cleanup();
        remoteTest(dataBaseLen, items, "", null, null, null);
        cleanup();
        remoteTest(dataBaseLen, items, "root", "", null, null);
        cleanup();
        remoteTest(dataBaseLen, items, "root", "123", "root", null);
        cleanup();
        remoteTest(dataBaseLen, items, "root", "123", "Root", null);
        cleanup();
        remoteTest(dataBaseLen, items, "root", "123", "Root", "123");
        cleanup();
        try {
            remoteTest(dataBaseLen, items, "root", "123", "Root", "");
            fail("Password error");
        } catch (WalException e) {
            // OK
        }
        cleanup();
        try {
            remoteTest(dataBaseLen, items, "root", "123", "Root", "12");
            fail("Password error");
        } catch (WalException e) {
            // OK
        }
        cleanup();
        try {
            remoteTest(dataBaseLen, items, "root", "abc", "Root", "Abc");
            fail("Password error");
        } catch (WalException e) {
            // OK
        }
        remoteTest(dataBaseLen, items, "root", "abc", "Root", "abc");
    }

    private void inprocTest(int dataBaseLen, int items, boolean testMasterDown) {
        File testDir = getDir();
        File masterDir = getDir(testDir, "master");
        File slaveDir = getDir(testDir, "slave");
        String url = "walog:inproc:slave:" + masterDir + "?dataDir=" + slaveDir;

        Runnable down = new Runnable() {
            @Override
            public void run() {
                IoUtils.close(master);
            }
        };
        Runnable reboot = new Runnable() {
            @Override
            public void run() {
                master = slave.getMaster();
            }
        };
        slaveTest("inproc", dataBaseLen, items, url, null, testMasterDown, down, reboot);
    }

    private void remoteTest(int dataBaseLen, int items, boolean testMasterDown) {
        remoteTest(dataBaseLen, items, null, null, null, null, testMasterDown);
    }

    private void remoteTest(int dataBaseLen, int items, String user, String password,
                            String masterUser, String masterPassword) {
        remoteTest(dataBaseLen, items, user, password, masterUser, masterPassword, false);
    }

    private void remoteTest(int dataBaseLen, int items, String user, String password,
                            String masterUser, String masterPassword, boolean testMasterDown) {
        File testDir = getDir();
        File masterDir = getDir(testDir, "master");
        File slaveDir = getDir(testDir, "slave");
        String url = "walog:rmi:slave://localhost/wal?dataDir=" + slaveDir
                + (user == null? "": "&user="+user);
        Properties info = new Properties();
        if (password != null) {
            info.put("password", password);
        }

        final String[] args;
        if (masterUser != null && masterPassword != null) {
            args = new String[] {"--proto", "rmi", "-d", masterDir + "", "-u", masterUser, "-p", masterPassword};
        } else if (masterUser != null) {
            args = new String[] {"--proto", "rmi", "-d", masterDir + "", "-u", masterUser};
        } else if (masterPassword != null) {
            args = new String[] {"--proto", "rmi", "-d", masterDir + "", "-p", masterPassword};
        } else {
            args = new String[] {"--proto", "rmi", "-d", masterDir + ""};
        }

        try {
            walServer = WalServer.boot(args);
            Runnable down = new Runnable() {
                @Override
                public void run() {
                    IoUtils.close(walServer);
                }
            };
            Runnable reboot = new Runnable() {
                @Override
                public void run() {
                    if (!walServer.isOpen()) {
                        walServer = WalServer.boot(args);
                    }
                }
            };
            slaveTest("rmi", dataBaseLen, items, url, info, testMasterDown, down, reboot);
        } catch (WalException e) {
            throw e;
        } catch (Exception e) {
            throw new AssertionError(e);
        } finally {
            IoUtils.close(walServer);
        }
    }

    private void slaveTest(String tag, int dataBaseLen, int items, String url, Properties info,
                           boolean testMasterDown, final Runnable down, final Runnable reboot) {
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
                IoUtils.info("%s: replicate from the start wal: items %d, data min size %d, test master down %s",
                        tag, items, dataBaseLen, testMasterDown);
            } else {
                assert fromWal != null;
                long lsn = fromWal.getLsn();
                IoUtils.info("%s: replicate from the wal 0x%x: items %d, data min size %d, test master down %s",
                        tag, lsn, case2Items, dataBaseLen, testMasterDown);
            }

            this.slave = WalDriverManager.connect(url, info);
            try {
                this.master = this.slave.getMaster();
                final int n;
                int i;
                if (caseIt == 0) {
                    i = 0;
                    n = items;
                } else {
                    n = items + case2Items;
                    i = items;
                }

                IoUtils.info("%s: write into master", tag);
                final boolean remote = "rmi".equals(tag);
                boolean downed = false;
                while (i < n) {
                    if (testMasterDown && !downed && i % (remote? 1000: 100) == 0) {
                        down.run();
                        downed = true;
                    }
                    try {
                        this.master.append(prefix + i);
                        downed = false;
                        ++i;
                    } catch (WalException e) {
                        if (!downed) throw e;
                        reboot.run();
                        this.master = this.slave.getMaster();
                    }
                }

                IoUtils.info("%s: read from slave and check", tag);
                Wal wal;
                if (caseIt == 0) {
                    i = 0;
                    wal = this.slave.first(0);
                } else {
                    i = items;
                    wal = this.slave.next(fromWal, 0);
                }
                downed = false;
                do {
                    String log  = prefix + i;
                    String data = new String(wal.getData(), Wal.CHARSET);
                    asserts(log.equals(data), "Replicated wal not matched: i = " +i);
                    if (testMasterDown && i % (remote? 2000: 200) == 0) {
                        down.run();
                        downed = true;
                    }
                    if (!downed && i + 1 >= n) {
                        reboot.run();
                        break;
                    }
                    if (downed) {
                        reboot.run();
                        downed = false;
                    }
                    try {
                        wal = this.slave.next(wal, 10000);
                        ++i;
                    } catch (TimeoutWalException e) {
                        wal = this.slave.next(wal);
                        if (wal == null) {
                            String s = "caseIt."+caseIt+" i = " + i + ", slave state " + this.slave.getState();
                            throw new AssertionError(s, e);
                        } else {
                            ++i;
                            // fs watch issue?
                            IoUtils.error("Wal exists but fetch timeout...", e);
                        }
                    }
                    fromWal = wal;
                } while (true);

                long diff = this.slave.bytesBehindMaster();
                asserts(diff == 0L, "case-"+caseIt+": diff " + diff);

                ++caseIt;
            } finally {
                IoUtils.close(this.slave);
            }
        }
    }

}
