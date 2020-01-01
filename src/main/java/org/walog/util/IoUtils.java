/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 little-pan
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.walog.util;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author little-pan
 * @since 2019-12-23
 *
 */
public final class IoUtils {
    
    private IoUtils() {
        // NOOP
    }

    public static int readInt(byte[] buffer) {
        return readInt(buffer, 0);
    }

    public static int readInt(byte[] buffer, int offset) {
        int i = buffer[offset++] & 0xff;
        i |= (buffer[offset++] & 0xff) << 8;
        i |= (buffer[offset++] & 0xff) << 16;
        i |= (buffer[offset]   & 0xff) << 24;
        return i;
    }

    public static void writeInt(int i, byte[] buffer, int offset) {
        buffer[offset++] = (byte)(i & 0xff);
        buffer[offset++] = (byte)((i >>>  8) & 0xff);
        buffer[offset++] = (byte)((i >>> 16) & 0xff);
        buffer[offset]   = (byte)((i >>> 24) & 0xff);
    }

    public static void readFully(FileChannel chan, ByteBuffer buffer, long pos)
            throws IOException {
        for (; buffer.hasRemaining(); ) {
            final int i = chan.read(buffer, pos);
            if (i == -1) {
                throw new EOFException();
            }
            pos += i;
        }
    }

    public static int getFletcher32(byte[] bytes) {
        return getFletcher32(bytes, 0, bytes.length);
    }

    /**
     * Calculate the Fletcher32 checksum.
     *
     * @param bytes the bytes
     * @param offset initial offset
     * @param length the message length (if odd, 0 is appended)
     * @return the checksum
     */
    public static int getFletcher32(byte[] bytes, int offset, int length) {
        int s1 = 0xffff, s2 = 0xffff;
        int i = offset, len = offset + (length & ~1);
        while (i < len) {
            // reduce after 360 words (each word is two bytes)
            for (int end = Math.min(i + 720, len); i < end;) {
                int x = ((bytes[i++] & 0xff) << 8) | (bytes[i++] & 0xff);
                s2 += s1 += x;
            }
            s1 = (s1 & 0xffff) + (s1 >>> 16);
            s2 = (s2 & 0xffff) + (s2 >>> 16);
        }
        if ((length & 1) != 0) {
            // odd length: append 0
            int x = (bytes[i] & 0xff) << 8;
            s2 += s1 += x;
        }
        s1 = (s1 & 0xffff) + (s1 >>> 16);
        s2 = (s2 & 0xffff) + (s2 >>> 16);
        return (s2 << 16) | s1;
    }
    
    public static void close(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

}
