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

package org.walog;

import java.io.*;

import org.walog.internal.NioWaler;
import org.walog.util.IoUtils;

/**
 * @author little-pan
 * @since 2019-12-22
 *
 */
public class WalerFactory {
    
    public static Waler open(File dir) throws WalException {
        final Waler waler = new NioWaler(dir);
        waler.open();
        return waler;
    }
    
    public static Waler open(String dir) throws WalException {
        return open(new File(dir));
    }

    public static Waler open(File dir, WalSlave slave) throws WalException {
        if (slave == null) {
            throw new NullPointerException("slave must be given");
        }

        final NioWaler waler = new NioWaler(dir, slave);
        waler.open();
        boolean failed = true;
        try {
            slave.open(waler);
            failed = false;
            return waler;
        } finally {
            if (failed) {
                IoUtils.close(waler);
            }
        }
    }

    public static Waler open(String dir, WalSlave slave) throws WalException {
        return open(new File(dir), slave);
    }

}
