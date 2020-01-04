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

package org.walog.util;

public final class UnitUtils {

    private UnitUtils() {

    }

    /** Parse ‘[0x]N[{g|G|m|M|k|K|b|B}]’ byte count format.
     *
     * @param value a string of byte count
     * @return byte count
     */
    public static long parseBytes(String value) {
        value = value.trim();
        final int length = value.length();
        int i = length - 1;
        final long scale;
        final char unit = value.charAt(i);
        switch (unit) {
            case 'g':
            case 'G':
                scale = 1L << 30;
                break;
            case 'm':
            case 'M':
                scale = 1L << 20;
                break;
            case 'k':
            case 'K':
                scale = 1L << 10;
                break;
            case 'b':
            case 'B':
                scale = 1L;
                break;
            default:
                if (unit < '0' || unit > '9') {
                    throw new IllegalArgumentException("value: " + value);
                }
                i = length;
                scale = 1L;
                break;
        }

        value = value.substring(0, i);
        return (Long.decode(value) * scale);
    }

}
