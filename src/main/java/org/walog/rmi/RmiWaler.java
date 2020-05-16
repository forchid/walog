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

public class RmiWaler implements Waler {

    protected final RmiWrapper wrapper;

    public RmiWaler(RmiWrapper wrapper) {
        this.wrapper = wrapper;
    }

    @Override
    public void open() throws WalException {
        // NOOP
    }

    @Override
    public Wal append(byte[] log) throws WalException {
        try {
            return this.wrapper.append(log);
        } catch (RemoteException e) {
            throw new IOWalException("append failed", e);
        }
    }

    @Override
    public Wal append(byte[] log, int offset, int length) throws WalException {
        try {
            return this.wrapper.append(log, offset, length);
        } catch (RemoteException e) {
            throw new IOWalException("append failed", e);
        }
    }

    @Override
    public Wal append(String log) throws WalException {
        try {
            return this.wrapper.append(log);
        } catch (RemoteException e) {
            throw new IOWalException("append failed", e);
        }
    }

    @Override
    public Wal first() throws WalException {
        try {
            return this.wrapper.first();
        } catch (RemoteException e) {
            throw new IOWalException("fetch first wal failed", e);
        }
    }

    @Override
    public Wal first(long timeout) throws WalException {
        try {
            return this.wrapper.first(timeout);
        } catch (RemoteException e) {
            throw new IOWalException("fetch first wal failed", e);
        }
    }

    @Override
    public Wal get(long lsn) throws WalException, IllegalArgumentException {
        try {
            return this.wrapper.get(lsn);
        } catch (RemoteException e) {
            throw new IOWalException("fetch specified wal failed", e);
        }
    }

    @Override
    public Wal next(Wal wal) throws WalException, IllegalArgumentException {
        try {
            return this.wrapper.next(wal);
        } catch (RemoteException e) {
            throw new IOWalException("fetch next wal failed", e);
        }
    }

    @Override
    public Wal next(Wal wal, long timeout) throws WalException, IllegalArgumentException {
        try {
            return this.wrapper.next(wal, timeout);
        } catch (RemoteException e) {
            throw new IOWalException("fetch specified wal failed", e);
        }
    }

    @Override
    public WalIterator iterator() {
        RmiIteratorWrapper wrapper = null;
        boolean failed = true;
        try {
            wrapper = this.wrapper.iterator();
            WalIterator it = new RmiWalIterator(wrapper);
            failed = false;
            return it;
        } catch (RemoteException e) {
            throw new IOWalException("Create wal iterator failed", e);
        } finally {
            if (failed) {
                IoUtils.close(wrapper);
            }
        }
    }

    @Override
    public WalIterator iterator(long lsn) throws IllegalArgumentException {
        RmiIteratorWrapper wrapper = null;
        boolean failed = true;
        try {
            wrapper = this.wrapper.iterator(lsn);
            WalIterator it = new RmiWalIterator(wrapper);
            failed = false;
            return it;
        } catch (RemoteException e) {
            throw new IOWalException("Create wal iterator failed", e);
        } finally {
            if (failed) {
                IoUtils.close(wrapper);
            }
        }
    }

    @Override
    public WalIterator iterator(long lsn, long timeout) throws IllegalArgumentException {
        RmiIteratorWrapper wrapper = null;
        boolean failed = true;
        try {
            wrapper = this.wrapper.iterator(lsn, timeout);
            WalIterator it = new RmiWalIterator(wrapper);
            failed = false;
            return it;
        } catch (RemoteException e) {
            throw new IOWalException("Create wal iterator failed", e);
        } finally {
            if (failed) {
                IoUtils.close(wrapper);
            }
        }
    }

    @Override
    public boolean purgeTo(String filename) throws WalException {
        try {
            return this.wrapper.purgeTo(filename);
        } catch (RemoteException e) {
            throw new IOWalException("purge to file failed", e);
        }
    }

    @Override
    public boolean purgeTo(long fileLsn) throws WalException {
        try {
            return this.wrapper.purgeTo(fileLsn);
        } catch (RemoteException e) {
            throw new IOWalException("purge to file failed", e);
        }
    }

    @Override
    public boolean clear() throws WalException {
        try {
            return this.wrapper.clear();
        } catch (RemoteException e) {
            throw new IOWalException("clear wal files failed", e);
        }
    }

    @Override
    public void sync() throws WalException {
        try {
            this.wrapper.sync();
        } catch (RemoteException e) {
            throw new IOWalException("sync failed", e);
        }
    }

    @Override
    public Wal last() throws WalException {
        try {
            return this.wrapper.last();
        } catch (RemoteException e) {
            throw new IOWalException("fetch last wal failed", e);
        }
    }

    @Override
    public boolean isOpen() {
        try {
            return this.wrapper.isOpen();
        } catch (RemoteException e) {
            throw new IOWalException("check open state failed", e);
        }
    }

    @Override
    public void close() {
        try {
            this.wrapper.close();
        } catch (RemoteException e) {
            throw new IOWalException("close failed", e);
        }
    }

}
