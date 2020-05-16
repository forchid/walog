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

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Properties;

public class RmiWalDriver extends AbstractWalDriver {

    static final String PREFIX = URL_PREFIX + "rmi:";

    static {
        WalDriver driver = new RmiWalDriver();
        WalDriverManager.registerDriver(driver);
    }

    private RmiWalDriver() {

    }

    @Override
    public <T extends Waler> T connect(String url, Properties info) throws WalException {
        if (!acceptsURL(url)) {
            return null;
        }
        int n = PREFIX.length();
        if (url.length() <= n || url.charAt(n) != '/') {
            return null;
        }

        RmiWrapper wrapper = null;
        boolean failed = true;
        try {
            Properties params = UrlUtils.parseParameters(url);
            if (info == null) {
                info = params;
            } else {
                info.putAll(params);
            }
            String rmiURL = url.substring(URL_PREFIX.length());
            int i = rmiURL.indexOf('?');
            if (i != -1) {
                rmiURL = rmiURL.substring(0, i);
            }
            RmiWalService service = (RmiWalService)Naming.lookup(rmiURL);
            wrapper = service.connect(info);
            if (wrapper == null) {
                throw new WalException("login error");
            }
            Waler waler = new RmiWaler(wrapper);
            failed = false;
            return cast(waler);
        } catch (NotBoundException | MalformedURLException | RemoteException e) {
            throw new WalException("lookup remote waler error", e);
        } finally {
            if (failed) {
                IoUtils.close(wrapper);
            }
        }
    }

    @Override
    protected String getPrefix() {
        return PREFIX;
    }

}
