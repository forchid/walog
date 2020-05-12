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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class InProcWalMaster extends WalMaster {

    protected final InProcWalSlave slave;
    final BlockingQueue<ByteBuffer> bufferQueue;

    public InProcWalMaster(Waler waler, InProcWalSlave slave) {
        super(waler);
        this.slave = slave;
        this.bufferQueue = new LinkedBlockingQueue<>();
    }

    @Override
    public void onActive() {
        super.onActive();

        Thread replicator = new Thread(this.replicate, "master-replicator");
        replicator.setDaemon(true);
        replicator.start();
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
        ByteBuffer buf =  this.slave.allocate(buffer.remaining());
        buf.put(buffer);
        buf.flip();
        this.slave.receive(buf);
    }

    @Override
    public void receive(ByteBuffer buffer) throws WalException {
        this.bufferQueue.offer(buffer);
    }

    protected void handle(ByteBuffer buffer) {
        super.receive(buffer);
    }

    final Runnable replicate = new Runnable() {

        @Override
        public void run() {
            long timeout = 1000L;
            ByteBuffer buffer;

            try {
                while (isOpen()) {
                    buffer =  bufferQueue.poll(timeout, TimeUnit.MILLISECONDS);
                    if (buffer != null) {
                        handle(buffer);
                    }
                }
            } catch (InterruptedException e) {
                // exit
            }
        }

    };

}
