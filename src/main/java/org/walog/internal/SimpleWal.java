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

public class SimpleWal implements Wal {

    protected final long lsn;
    final byte prefix;
    protected final byte[] data;

    protected SimpleWal(long lsn, byte prefix, byte[] data) {
        this.lsn = lsn;
        this.prefix = prefix;
        this.data= data;
    }

    int getHeadSize() {
        final int p = this.prefix & 0xff;
        if (p < 0xfb) {
            return 1;
        } else if (p == 0xfc) {
            return 3;
        } else {
            return 4;
        }
    }

    public int getOffset() {
        return WalFileUtils.fileOffset(this.lsn);
    }

    public long nextLsn() {
        final int offset = getOffset();
        final int nextOffset = offset + getHeadSize() + getData().length + 8;
        if (nextOffset < 0 || nextOffset > Wal.LSN_OFFSET_MASK) {
            throw new IllegalStateException("offset full");
        }

        return (WalFileUtils.fileLsn(this.lsn) + nextOffset);
    }

    @Override
    public long getLsn() {
        return this.lsn;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }

}
