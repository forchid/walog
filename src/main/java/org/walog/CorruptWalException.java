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

public class CorruptWalException extends RuntimeException {

    protected final String file;
    protected final int offset;

    public CorruptWalException(String message, String file, int offset) {
        super(message);

        this.file = file;
        this.offset = offset;
    }

    @Override
    public String getMessage() {
        String base = super.getMessage();
        return String.format("%s - offset 0x%x in '%s'", base, getOffset(), getFile());
    }

    @Override
    public String toString() {
        String className = getClass().getName();
        return (className+ ": "+getMessage());
    }

    public String getFile() {
        return file;
    }

    public int getOffset() {
        return offset;
    }

}
