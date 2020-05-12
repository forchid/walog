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

import org.walog.internal.NioWaler;
import org.walog.util.IoUtils;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public abstract class WalSlave implements WalNode {

    public static final int STATE_UNINITED  = 0;
    public static final int STATE_INITED    = 1;
    public static final int STATE_WAITING   = 2;
    public static final int STATE_APPENDING = 4;
    public static final int STATE_CLOSED    = 8;

    protected volatile NioWaler waler;
    protected SimpleWal currWal;
    protected final AtomicLong lastLsn;

    private volatile int state = STATE_UNINITED;
    private boolean active;

    protected WalSlave(SimpleWal currWal) {
        this.currWal = currWal;
        this.lastLsn = new AtomicLong(-1L);
    }

    public int getState() {
        return this.state;
    }

    @Override
    public boolean isOpen() {
        return (STATE_CLOSED != getState());
    }

    /** Byte count behind wal master.
     *
     * @return byte count, or -1L if no wal received from master after open
     */
    public long bytesBehindMaster() {
        final long lastLsn = this.lastLsn.get();
        if (lastLsn == -1L) {
            return -1L;
        } else {
            return (lastLsn - this.currWal.getLsn());
        }
    }

    @Override
    public void onActive() {
        if (this.waler == null || !this.waler.isOpen()) {
            throw new IllegalStateException("waler isn't open");
        }
        if (this.active) {
            throw new IllegalStateException("Wal slave was active");
        }
        this.active = true;

        long currLsn = BEGIN_LSN;
        if (this.currWal != null) {
            currLsn = this.currWal.getLsn();
        }
        ByteBuffer buffer = allocate();
        buffer.put(CMD_FETCH_FROM);
        buffer.putLong(currLsn);

        buffer.flip();
        send(buffer);
    }

    @Override
    public void onInactive() {
        this.state = STATE_CLOSED;
        IoUtils.close(this.waler);
        this.waler = null;
        this.active = false;
    }

    @Override
    public void receive(ByteBuffer buffer) throws WalException {
        if (!isOpen()) {
            throw new NodeWalException("Wal slave closed");
        }

        boolean failed = true;
        try {
            for (;;) {
                final int rem = buffer.remaining();

                // Packet format: length(int) + status(byte) [+ data-field(n bytes)]
                if (rem < 4) {
                    IoUtils.debug("Result too short(%d bytes) and wait more", rem);
                    failed = false;
                    return;
                }
                int pos = buffer.position();
                int length = buffer.getInt();
                int size = rem - 4;
                if (size < length) {
                    IoUtils.debug("Packet too short(%d bytes) and wait more(%d bytes)", size, length);
                    buffer.position(pos);
                    failed = false;
                    return;
                }

                byte status = buffer.get();
                size = rem - 5;
                switch (status) {
                    case RES_WAL:
                        Wal wal = parseWalPacket(buffer, size);
                        if (wal == null) { // Wait more data
                            buffer.position(pos);
                            failed = false;
                            return;
                        }
                        break;
                    case RES_ERR:
                        parseErrPacket(buffer, size);
                        break;
                    default:
                        String error = String.format("Unknown status in wal packet: 0x%02X", status);
                        throw new NodeWalException(error);
                }
            }
        } finally {
            if (!failed && STATE_UNINITED != this.state) {
                this.state = STATE_WAITING;
            }
        }
    }

    protected Wal parseWalPacket(ByteBuffer buffer, int size) throws WalException {
        IoUtils.debug("parse wal packet: size %d", size);
        // wal pack: last-lsn(long) + wal-lsn(long) + wal-item(var)
        // wal item: wal-length(var-int) + wal-payload + wal-offset(int) + wal-checksum(int)
        if (size < 24 + 1/* wal length prefix */) {
            IoUtils.debug("Wal packet too short(%d bytes) and wait more(25 bytes)", size);
            return null;
        }

        final int p = buffer.position();
        int i = 16;
        // Do parse wal packet
        int prefix = buffer.get(p + i++) & 0xff;
        int headSize = SimpleWal.headSize((byte)prefix);
        int expectSize = 24 + headSize;
        if (size < expectSize) {
            IoUtils.debug("Wal packet too short(%d bytes) and wait more(%d bytes)", size, expectSize);
            return null;
        }

        int length;
        if (prefix < 0xfb) {
            length = prefix;
        } else if (prefix == 0xfc) {
            length  =  buffer.get(p + i++) & 0xff;
            length |= (buffer.get(p + i)   & 0xff) << 8;
        } else {
            length  =  buffer.get(p + i++) & 0xff;
            length |= (buffer.get(p + i++) & 0xff) << 8;
            length |= (buffer.get(p + i)   & 0xff) << 16;
        }
        expectSize = 24 + headSize + length;
        if (size < expectSize) {
            IoUtils.debug("Wal packet too short(%d bytes) and wait more(%d bytes)", size, expectSize);
            return null;
        }

        final long lastLsn, walLsn;
        final int offset, chkSum;
        i = 0;
        lastLsn = buffer.getLong(p + i);
        i += 8;
        walLsn = buffer.getLong(p + i);
        i += 8 + headSize;
        final byte[] data = new byte[length];
        for (int j = 0; j < length; ++j) {
            data[j] = buffer.get(p + i + j);
        }
        i += length;
        offset = buffer.getInt(p + i);
        i += 4;
        chkSum = buffer.getInt(p + i);
        i += 4;
        buffer.position(p + i);

        // Check integrity
        if (chkSum != IoUtils.getFletcher32(data)) {
            throw new NodeWalException("Wal corrupted from wal master");
        }
        SimpleWal wal = new SimpleWal(walLsn, (byte)prefix, data);
        if (offset != wal.getOffset()) {
            throw new NodeWalException("Wal offset not matched with lsn from wal master");
        }

        // Handle wal item
        final SimpleWal currWal = this.currWal;
        if (this.lastLsn.get() == -1L && currWal != null) {
            // 0) Init after open
            if (walLsn != currWal.getLsn()) {
                throw new NodeWalException("First wal from master not matched the current");
            }
            this.currWal = wal;
            this.lastLsn.set(lastLsn);
            this.state = STATE_INITED;
        } else {
            // 1) Continuous replication
            if (currWal != null && walLsn != currWal.nextLsn() && walLsn != currWal.nextFileLsn()) {
                throw new NodeWalException("Continuous replication violated");
            }
            onReceived(wal);
            IoUtils.debug("append received wal: %s", wal);
            this.state = STATE_APPENDING;
            this.waler.append(wal);
            this.currWal = wal;
            this.lastLsn.set(lastLsn);
            onAppended(wal);
        }

        return wal;
    }

    protected void parseErrPacket(ByteBuffer buffer, int size) throws WalException {
        String error = "Wal master error";
        if (size > 0) {
            byte[] errBuf = new byte[size];
            buffer.get(errBuf);
            error = new String(errBuf, Wal.CHARSET);
        }

        throw new NodeWalException(error);
    }

    void open(NioWaler waler) {
        this.waler = waler;
        onOpen(waler);
    }

    protected void onOpen(Waler waler) {

    }

    protected void onReceived(Wal wal) {

    }

    protected void onAppended(Wal wal) {

    }

}
