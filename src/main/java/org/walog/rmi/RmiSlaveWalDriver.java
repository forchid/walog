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

package org.walog.rmi;

import org.walog.*;
import org.walog.util.IoUtils;
import org.walog.util.UrlUtils;

import java.util.Properties;

/** A wal driver that builds a waler, which replicates the specified rmi waler
 * into it's data directory automatically. Eg: "walog:rmi:slave://localhost/wal?dataDir=data".
 *
 * @since 2020-05-16
 * @author little-pan
 */
public class RmiSlaveWalDriver extends AbstractWalDriver {

    static final String PREFIX = URL_PREFIX + "rmi:slave:";

    static {
        WalDriver driver = new RmiSlaveWalDriver();
        WalDriverManager.registerDriver(driver);
    }

    private RmiSlaveWalDriver() {

    }

    @Override
    public <T extends Waler> T connect(String url, Properties info) throws WalException {
        if (!acceptsURL(url)) {
            return null;
        }

        Properties params = UrlUtils.parseParameters(url);
        Properties props = new Properties();
        if (info != null) {
            props.putAll(info);
        }
        props.putAll(params);
        String dataDir = props.getProperty("dataDir");
        if (dataDir == null || dataDir.length() == 0) {
            throw new WalException("No dataDir parameter");
        }

        String prefix = getPrefix();
        String masterURL = url.substring(prefix.length());
        masterURL = RmiWalDriver.PREFIX + masterURL;
        Waler master = WalDriverManager.connect(masterURL, info);
        if (master == null) {
            return null;
        }

        SlaveWaler waler = null;
        boolean failed = true;
        try {
            waler = new SlaveWaler(master, dataDir, params);
            waler.open();
            failed = false;
            return cast(waler);
        } finally {
            if (failed) {
                IoUtils.close(waler);
                IoUtils.close(master);
            }
        }
    }

    @Override
    protected String getPrefix() {
        return PREFIX;
    }

}
