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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class InProcWalSlave extends WalSlave {

    protected InProcWalMaster master;
    protected ByteBuffer lastBuffer;

    public InProcWalSlave(Wal currWal) {
        super(currWal);
    }

    public void setMaster(InProcWalMaster master) {
        this.master = master;
    }

    @Override
    public ByteBuffer allocate() {
        return wrapBuffer(allocate(4 << 10));
    }

    @Override
    public ByteBuffer allocate(int capacity) {
        return wrapBuffer(ByteBuffer.allocate(capacity));
    }

    protected ByteBuffer wrapBuffer(ByteBuffer buffer) {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        return buffer;
    }

    @Override
    public void send(ByteBuffer buffer) throws WalException {
        ByteBuffer buf = this.master.allocate(buffer.remaining());
        buf.put(buffer);
        buf.flip();
        this.master.receive(buf);
    }

    @Override
    public void receive(ByteBuffer buffer) throws WalException {
        ByteBuffer last = this.lastBuffer;

        if (last == null) {
            last = buffer;
        } else {
            int lastRem = last.remaining();
            int bufRem = buffer.remaining();
            if (last.capacity() - lastRem >= bufRem) {
                last.position(lastRem);
                last.limit(lastRem + bufRem);
                last.put(buffer);
            } else {
                int newCap = lastRem + bufRem;
                ByteBuffer newBuf = allocate(newCap);
                newBuf.put(last);
                newBuf.put(buffer);
                last = newBuf;
            }
            last.flip();
        }

        super.receive(last);
        if (last.hasRemaining()) {
            if (last.position() != 0) {
                last.compact();
                last.flip();
            }
            this.lastBuffer = last;
        } else {
            this.lastBuffer = null;
        }
    }

}
