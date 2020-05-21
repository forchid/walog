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

package org.walog.rmi;

import org.walog.*;

import java.rmi.RemoteException;
import java.util.Iterator;

public class RmiWalIterator implements WalIterator {

    protected final RmiIteratorWrapper wrapper;
    // Optimize: batch iterate
    protected Iterator<SimpleWal> nextIt;
    private boolean hasNextCalled;

    public RmiWalIterator(RmiIteratorWrapper wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public boolean hasNext() throws WalException {
        try {
            this.hasNextCalled = true;
            Iterator<SimpleWal> nextIt = this.nextIt;
            if (nextIt != null && nextIt.hasNext()) {
                return true;
            }
            this.nextIt = null;
            return this.wrapper.hasNext();
        } catch (RemoteException e) {
            throw new NetWalException("call hasNext() error", e);
        }
    }

    @Override
    public Wal next() throws WalException {
        try {
            if (!this.hasNextCalled) {
                throw new IllegalStateException("haxNext() not called");
            }
            this.hasNextCalled = false;

            Iterator<SimpleWal> nextIt = this.nextIt;
            if (nextIt != null && nextIt.hasNext()) {
                return nextIt.next();
            } else {
                SimpleWal next = this.wrapper.next();
                this.nextIt = next.iterator();
                return next;
            }
        } catch (RemoteException e) {
            throw new NetWalException("call next() error", e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("wal iterator read only");
    }

    @Override
    public boolean isOpen() {
        try {
            return this.wrapper.isOpen();
        } catch (RemoteException e) {
            throw new NetWalException("call isOpen() error", e);
        }
    }

    @Override
    public void close() {
        try {
            this.wrapper.close();
        } catch (RemoteException e) {
            throw new NetWalException("call close() error", e);
        }
    }

}
