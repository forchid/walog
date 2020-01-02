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

package org.walog.internal;

import org.walog.Wal;
import org.walog.util.WalFileUtils;

import java.io.IOException;
import java.util.Iterator;

class NioWalIterator implements Iterator<Wal> {

    protected final NioWaler waler;
    protected long lsn;
    protected NioWalFile walFile;
    protected SimpleWal wal;
    private boolean hasNextCalled;

    public NioWalIterator(NioWaler waler, long lsn) {
        this.waler = waler;
        this.lsn   = lsn;
    }

    @Override
    public boolean hasNext() {
        this.hasNextCalled = true;
        if (this.wal != null) {
            return true;
        }
        try {
            if (this.walFile == null) {
                // Open wal file
                this.walFile = this.waler.getWalFile(this.lsn);
                if (this.walFile == null) {
                    return false;
                }
            }

            this.wal = this.walFile.get(this.lsn);
            if (this.wal == null) {
                // Open next wal file
                this.lsn = WalFileUtils.nextFileLsn(this.lsn);
                this.walFile = this.waler.getWalFile(this.lsn);
                if (this.walFile == null) {
                    return false;
                }
                this.wal = walFile.get(this.lsn);
            }

            if (this.wal != null) {
                this.lsn = this.wal.nextLsn();
                return true;
            }
            return false;
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public SimpleWal next() {
        if (!this.hasNextCalled) {
            throw new IllegalStateException("haxNext() not called");
        }

        this.hasNextCalled  = false;
        final SimpleWal res = this.wal;
        this.wal = null;
        return res;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
