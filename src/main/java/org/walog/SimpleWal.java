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

import org.walog.util.WalFileUtils;

public class SimpleWal implements Wal {

    protected final long lsn;
    final byte prefix;
    protected final byte[] data;

    public SimpleWal(long lsn, byte prefix, byte[] data) {
        this.lsn = lsn;
        this.prefix = prefix;
        this.data = data;
    }

    public static byte lengthPrefix(byte[] data) throws IllegalArgumentException {
        byte pfx;
        int length = data.length;

        if (length >= 1 << 24) {
            throw new IllegalArgumentException("data too long");
        }

        if (length >= 1 << 16) {
            pfx = (byte)0xfd;
        } else if (length >= 0xfb) {
            pfx = (byte)0xfc;
        } else {
            pfx = (byte)length;
        }

        return pfx;
    }

    public static int headSize(byte lengthPrefix) {
        int p = lengthPrefix & 0xff;

        if (p < 0xfb) {
            return 1;
        } else if (p == 0xfc) {
            return 3;
        } else if (p == 0xfd) {
            return 4;
        } else {
            String message = "Illegal prefix of wal length: " + Integer.toHexString(p);
            throw new IllegalArgumentException(message);
        }
    }

    int getHeadSize() {
        return headSize(this.prefix);
    }

    public int getOffset() {
        return WalFileUtils.fileOffset(this.lsn);
    }

    protected int getNextOffset() {
        final int offset = getOffset();
        return (offset + getHeadSize() + this.data.length + 8);
    }

    public int nextOffset() throws WalException {
        final int nextOffset = getNextOffset();
        if (nextOffset < 0 || nextOffset > Wal.LSN_OFFSET_MASK/*Note: not ROLL_SIZE for a little overflow */) {
            throw new WalException("Offset full");
        }

        return nextOffset;
    }

    @Override
    public long nextLsn() throws WalException {
        final int nextOffset = nextOffset();
        return (WalFileUtils.fileLsn(this.lsn) + nextOffset);
    }

    @Override
    public long nextFileLsn() {
        return WalFileUtils.nextFileLsn(this.lsn);
    }

    @Override
    public long getLsn() {
        return this.lsn;
    }

    @Override
    public byte[] getData() {
        return this.data;
    }

    @Override
    public String toString() {
        return (new String(this.data, Wal.CHARSET));
    }

}
