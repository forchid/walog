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

import org.walog.util.IoUtils;
import java.util.*;

/** <p>
 * Wal driver manager that registers, deregister driver, and
 * select a driver to build wal connection for given url.
 * </p>
 *
 * <p>
 * The url format 'walog:PROTOCOL[:SUB-PROTOCOL]:PATH[?PARAMETER-NAME=PARAMETER-VALUE[&...]]'.
 * Built drivers include "walog:file:", "walog:rmi:[slave:]", and "walog:inproc:[slave:]".
 * </p>
 *
 * @since 2020-05-15
 * @author little-pan
 */
public class WalDriverManager {

    static final List<WalDriver> DRIVERS = new LinkedList<>();
    static final String BUILTS = "org.walog.internal.FileWalDriver:"
            +"org.walog.rmi.RmiWalDriver:org.walog.rmi.RmiSlaveWalDriver:"
            +"org.walog.inproc.InprocWalDriver:org.walog.inproc.InprocSlaveWalDriver";

    static {
        String drivers = System.getProperty("org.walog.drivers");
        if (drivers == null) {
            drivers = BUILTS;
        } else {
            drivers = drivers + ":" + BUILTS;
        }
        for (String d: drivers.split(":")) {
            String name = d.trim();
            if (name.length() > 0) {
                IoUtils.debug("Init wal driver: %s", name);
                try {
                    Class.forName(name);
                } catch (ClassNotFoundException e) {
                    IoUtils.debug("Wal driver '%s' not found", name);
                }
            }
        }
    }

    private WalDriverManager() {

    }

    public static <T extends Waler> T connect(String url) {
        return connect(url, null);
    }

    public static <T extends Waler> T connect(String url, String user, String password)
            throws WalException {

        Properties info = new Properties();
        info.put("user", user);
        info.put("password", password);
        return connect(url, info);
    }

    public static <T extends Waler> T connect(String url, Properties info)
            throws WalException {
        WalDriver driver = getDriver(url);

        if (driver == null) {
            throw new WalException("No wal driver found: url '" + url+ "'");
        }

        return driver.connect(url, info);
    }

    public static WalDriver getDriver(String url) {
        synchronized (DRIVERS) {
            for (WalDriver d : DRIVERS) {
                if (d.acceptsURL(url)) {
                    return d;
                }
            }
        }

        return null;
    }

    public static Iterator<WalDriver> iterator() {
        List<WalDriver> copy;
        synchronized (DRIVERS) {
            copy = new ArrayList<>(DRIVERS);
        }

        return copy.iterator();
    }

    public static void registerDriver(WalDriver driver) throws NullPointerException {
        if (driver == null) {
            throw new NullPointerException("driver null");
        }

        synchronized (DRIVERS) {
            DRIVERS.add(0, driver);
        }
    }

    public static void deregisterDriver(WalDriver driver) {
        synchronized (DRIVERS) {
            Iterator<WalDriver> it = DRIVERS.iterator();
            while (it.hasNext()) {
                WalDriver d = it.next();
                if (d == driver) {
                    it.remove();
                }
            }
        }
    }

}
