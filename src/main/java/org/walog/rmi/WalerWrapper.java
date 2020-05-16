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
import org.walog.util.IoUtils;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class WalerWrapper extends UnicastRemoteObject implements RmiWrapper {

    protected final Waler waler;
    private volatile boolean open;

    public WalerWrapper(Waler waler) throws RemoteException {
        this.waler = waler;
        this.open = true;
    }

    @Override
    public void open() throws WalException, RemoteException {
        ensureOpen();
    }

    @Override
    public Wal append(byte[] log) throws WalException, RemoteException {
        ensureOpen();
        return this.waler.append(log);
    }

    @Override
    public Wal append(byte[] log, int offset, int length) throws WalException, RemoteException {
        ensureOpen();
        return this.waler.append(log, offset, length);
    }

    @Override
    public Wal append(String log) throws WalException, RemoteException {
        ensureOpen();
        return this.waler.append(log);
    }

    @Override
    public Wal first() throws WalException, RemoteException {
        ensureOpen();
        return this.waler.first();
    }

    @Override
    public Wal first(long timeout) throws WalException, RemoteException {
        ensureOpen();
        return this.waler.first(timeout);
    }

    @Override
    public Wal get(long lsn) throws WalException, IllegalArgumentException, RemoteException {
        ensureOpen();
        return this.waler.get(lsn);
    }

    @Override
    public Wal next(Wal wal) throws WalException, IllegalArgumentException, RemoteException {
        ensureOpen();
        return this.waler.next(wal);
    }

    @Override
    public Wal next(Wal wal, long timeout) throws WalException, IllegalArgumentException, RemoteException {
        ensureOpen();
        return this.waler.next(wal, timeout);
    }

    @Override
    public RmiIteratorWrapper iterator() throws RemoteException {
        ensureOpen();

        WalIterator iterator = null;
        boolean failed = true;
        try {
            iterator = this.waler.iterator();
            WalIteratorWrapper it = new WalIteratorWrapper(iterator);
            failed = false;
            return it;
        } catch (RemoteException e) {
            throw new IOWalException("Create wal iterator failed", e);
        } finally {
            if (failed) {
                IoUtils.close(iterator);
            }
        }
    }

    @Override
    public RmiIteratorWrapper iterator(long lsn) throws IllegalArgumentException, RemoteException {
        ensureOpen();

        WalIterator iterator = null;
        boolean failed = true;
        try {
            iterator = this.waler.iterator(lsn);
            WalIteratorWrapper it = new WalIteratorWrapper(iterator);
            failed = false;
            return it;
        } catch (RemoteException e) {
            throw new IOWalException("Create wal iterator failed", e);
        } finally {
            if (failed) {
                IoUtils.close(iterator);
            }
        }
    }

    @Override
    public RmiIteratorWrapper iterator(long lsn, long timeout) throws IllegalArgumentException, RemoteException {
        ensureOpen();

        WalIterator iterator = null;
        boolean failed = true;
        try {
            iterator = this.waler.iterator(lsn, timeout);
            WalIteratorWrapper it = new WalIteratorWrapper(iterator);
            failed = false;
            return it;
        } catch (RemoteException e) {
            throw new IOWalException("Create wal iterator failed", e);
        } finally {
            if (failed) {
                IoUtils.close(iterator);
            }
        }
    }

    @Override
    public boolean purgeTo(String filename) throws WalException, RemoteException {
        ensureOpen();
        return false;
    }

    @Override
    public boolean purgeTo(long fileLsn) throws WalException, RemoteException {
        ensureOpen();
        return this.waler.purgeTo(fileLsn);
    }

    @Override
    public boolean clear() throws WalException, RemoteException {
        ensureOpen();
        return this.waler.clear();
    }

    @Override
    public void sync() throws WalException, RemoteException {
        ensureOpen();
        this.waler.sync();
    }

    @Override
    public Wal last() throws WalException, RemoteException {
        ensureOpen();
        return this.waler.last();
    }

    @Override
    public boolean isOpen() throws RemoteException {
        return this.open;
    }

    @Override
    public void close() throws RemoteException {
        this.open = false;
    }

    protected void ensureOpen() throws IOWalException, RemoteException {
        if (!isOpen()) {
            throw new IOWalException("Remote waler closed");
        }
    }

}
