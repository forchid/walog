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
import org.walog.util.IoUtils;
import org.walog.util.WalFileUtils;
import static org.walog.util.WalFileUtils.*;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

class NioWalIterator implements Iterator<Wal> {

    protected final NioWaler waler;
    protected long lsn;
    protected NioWalFile walFile;
    protected SimpleWal wal;
    private boolean hasNextCalled;
    private boolean noNext;

    public NioWalIterator(NioWaler waler, long lsn) {
        this.waler = waler;
        this.lsn   = lsn;
    }

    @Override
    public boolean hasNext() {
        this.hasNextCalled = true;
        if (this.noNext) {
            return false;
        }

        if (this.wal != null) {
            return true;
        }
        try {
            if (this.walFile == null) {
                // Open wal file
                this.walFile = this.waler.getWalFile(this.lsn);
                if (this.walFile == null) {
                    this.noNext = true;
                    return false;
                }
            }

            this.wal = this.walFile.get(this.lsn);
            if (this.wal == null) {
                // Open next wal file
                this.lsn = WalFileUtils.nextFileLsn(this.lsn);
                this.walFile = this.waler.getWalFile(this.lsn);
                if (this.walFile == null) {
                    this.noNext = true;
                    return false;
                }
                this.wal = walFile.get(this.lsn);
            }

            if (this.wal != null) {
                this.lsn = this.wal.nextLsn();
                return true;
            }

            this.noNext = true;
            return false;
        } catch (final IOException e) {
            if (e instanceof EOFException) {
                final File dir = this.waler.getDirectory();
                final String filename = this.walFile.getFilename();
                final long lastLsn = lastFileLsn(dir, filename);
                if (this.walFile.getLsn() == lastLsn) {
                    IoUtils.debug("Reach to the end of file '%s' in '%s'", filename, dir);
                    this.noNext = true;
                    return false;
                }
            }
            throw new IllegalStateException(e);
        }
    }

    @Override
    public SimpleWal next() {
        if (!this.hasNextCalled) {
            throw new IllegalStateException("haxNext() not called");
        }
        this.hasNextCalled = false;
        if (this.noNext) {
            throw new NoSuchElementException();
        }

        final SimpleWal res = this.wal;
        this.wal = null;
        return res;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
