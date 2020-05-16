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

package org.walog.inproc;

import org.walog.*;
import org.walog.util.IoUtils;

import java.util.Properties;

public class InprocWalDriver extends AbstractWalDriver {

    static final String PREFIX = URL_PREFIX + "inproc:";
    static final String SLAVE_PREFIX = PREFIX + "slave:";

    static {
        WalDriver driver = new InprocWalDriver();
        WalDriverManager.registerDriver(driver);
    }

    private InprocWalDriver() {

    }

    @Override
    public <T extends Waler> T connect(String url, Properties info) throws WalException {
        if (!acceptsURL(url)) {
            return null;
        }

        String dataDir = url.substring(PREFIX.length());
        int i = dataDir.indexOf('?');
        if (i != -1) {
            dataDir = dataDir.substring(0, i);
        }

        Waler waler = WalerFactory.open(dataDir);
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
    public boolean acceptsURL(String url) {
        return (url.startsWith(PREFIX) && !url.startsWith(SLAVE_PREFIX));
    }

    @Override
    protected String getPrefix() {
        return PREFIX;
    }

}
