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

import org.walog.*;
import org.walog.util.IoUtils;

import java.util.Properties;

public class FileWalDriver extends AbstractWalDriver {

    static final String PREFIX = URL_PREFIX + "file:";

    static {
        WalDriver driver = new FileWalDriver();
        WalDriverManager.registerDriver(driver);
    }

    protected FileWalDriver() {

    }

    @Override
    public <T extends Waler> T connect(String url, Properties info) throws WalException {
        if (!acceptsURL(url)) {
            return null;
        }

        String path = url.substring(PREFIX.length());
        int i = path.indexOf('?');
        if (i != -1) {
            path = path.substring(0, i);
        }

        Waler waler = WalerFactory.open(path);
        boolean failed = true;
        try {
            T t = cast(waler);
            failed = false;
            return t;
        } finally {
            if (failed) {
                IoUtils.close(waler);
            }
        }
    }

    @Override
    protected String getPrefix() {
        return PREFIX;
    }

}
