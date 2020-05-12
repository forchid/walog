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
import org.walog.util.WalFileUtils;

import java.nio.ByteBuffer;

public abstract class WalMaster implements WalNode, Runnable {

    public static final int STATE_UNINITED  = 0;
    public static final int STATE_WAITING   = 1;
    public static final int STATE_INITED    = 2;
    public static final int STATE_FETCHING  = 4;
    public static final int STATE_CLOSED    = 8;

    protected final Waler waler;

    private volatile int state = STATE_UNINITED;
    protected WalIterator iterator;
    private long currLsn;

    protected WalMaster(Waler waler) throws NullPointerException {
       if (waler == null) {
           throw new NullPointerException("waler must be given");
       }
        this.waler = waler;
    }

    @Override
    public boolean isOpen() {
        return (STATE_CLOSED != getState());
    }

    public int getState() {
        return this.state;
    }

    @Override
    public void onActive() {
        if (!this.waler.isOpen()) {
            throw new NodeWalException("waler closed");
        }
        this.state = STATE_WAITING;
    }

    @Override
    public void onInactive() {
        this.state = STATE_CLOSED;
        IoUtils.close(this.iterator);
    }

    @Override
    public void receive(ByteBuffer buffer) throws WalException {
        if (!isOpen()) {
            throw new NodeWalException("Wal master closed");
        }
        if (STATE_WAITING != getState()) {
            throw new NodeWalException("Wal master not active or initialized");
        }

        int rem = buffer.remaining();
        if (rem < 9) {
            IoUtils.debug("Command too short(%d bytes) then wait more", rem);
            return;
        }

        final byte cmd = buffer.get();
        if (CMD_FETCH_FROM != cmd) {
            sendErrPacket("Unknown command: " + Integer.toHexString(cmd));
            onInactive();
            return;
        }
        long currLsn = buffer.getLong();
        if (currLsn < -1L) {
            sendErrPacket("Error request lsn: " + currLsn);
            onInactive();
            return;
        }

        IoUtils.debug("Wal master initialized");
        this.state = STATE_INITED;
        this.currLsn = currLsn;
        Thread fetcher = new Thread(this, "wal-master");
        fetcher.setDaemon(true);
        fetcher.start();
    }

    @Override
    public void run() {
        IoUtils.debug("Wal fetcher running");
        final long timeout = 0L;

        try {
            // Init
            if (this.currLsn == -1L) {
                Wal first = this.waler.first(timeout);
                this.currLsn = first.getLsn();
            }
            WalIterator iterator = this.waler.iterator(this.currLsn, timeout);
            this.iterator = iterator;

            // Fetch
            this.state = STATE_FETCHING;
            while (isOpen() && iterator.hasNext()) {
                Wal wal = iterator.next();
                Wal last = this.waler.last();
                sendWalPacket(wal, last);
            }
        } catch (WalException e) {
            if (this.waler.isOpen()) {
                throw e;
            }
        }
    }

    protected void sendWalPacket(Wal wal, Wal last) {
        IoUtils.debug("Fetch: curr '%s', last '%s'", wal, last);
        // wal pack: last-lsn(long) + wal-lsn(long) + wal-item(var)
        // wal item: wal-length(var-int) + wal-payload + wal-offset(int) + wal-checksum(int)
        final byte[] data = wal.getData();
        final int dataLen = data.length;
        int prefix = SimpleWal.lengthPrefix(data) & 0xff;
        int headSize = SimpleWal.headSize((byte)prefix);
        int packLen = 1 + 8 + 8 + headSize + dataLen + 8;
        IoUtils.debug("Packet length %d", packLen);

        ByteBuffer buf = allocate( 4 + packLen);
        buf.putInt(packLen);
        buf.put(RES_WAL);
        buf.putLong(last.getLsn());
        buf.putLong(wal.getLsn());
        if (prefix < 0xfb) {
            buf.put((byte)prefix);
        } else if (prefix == 0xfc) {
            buf.put((byte)prefix);
            buf.put((byte)(dataLen));
            buf.put((byte)(dataLen << 8));
        } else {
            buf.put((byte)prefix);
            buf.put((byte)(dataLen));
            buf.put((byte)(dataLen << 8));
            buf.put((byte)(dataLen << 16));
        }
        buf.put(data);
        int offset = WalFileUtils.fileOffset(wal.getLsn());
        buf.putInt(offset);
        int chkSum = IoUtils.getFletcher32(data);
        buf.putInt(chkSum);

        buf.flip();
        send(buf);
    }

    protected void sendErrPacket(String message) {
        byte[] error = message.getBytes(Wal.CHARSET);
        ByteBuffer buf = allocate(4 + 1 + error.length);
        buf.putInt(1 + error.length);
        buf.put(RES_ERR);
        buf.put(error);

        buf.flip();
        send(buf);
    }

}
