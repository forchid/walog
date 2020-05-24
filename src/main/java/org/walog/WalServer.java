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

import org.walog.rmi.RmiWalServer;
import org.walog.util.IoUtils;

import java.util.Properties;

import static java.lang.Integer.getInteger;
import static java.lang.System.getProperty;

/** Wal server for local/remote connection.
 *
 * @since 2020-05-24
 * @author little-pan
 */
public abstract class WalServer implements AutoCloseable {

    static final String PROP_PREFIX = "org.walog.server";

    protected static final String PROTO = getProperty(PROP_PREFIX+".proto", "rmi");
    protected static final String HOST = getProperty(PROP_PREFIX+".host", "localhost");
    protected static final int PORT = getInteger(PROP_PREFIX+".port", 1099);
    protected static final String DIR  = getProperty(PROP_PREFIX+".dataDir", "data");
    protected static final String NAME = getProperty(PROP_PREFIX+".service", "walog");
    protected static final String USER = getProperty(PROP_PREFIX+".user");
    protected static final String PASSWORD = getProperty(PROP_PREFIX+".password");
    protected static final int FETCH_SIZE = getInteger(PROP_PREFIX+".fetchSize", 64);
    // Master config
    protected static final String MASTER_URL = getProperty(PROP_PREFIX+".master.url");
    protected static final String MASTER_USER = getProperty(PROP_PREFIX+".master.user");
    protected static final String MASTER_PASSWORD = getProperty(PROP_PREFIX+".master.password");

    public static void main(String[] args) {
        boot(args);
    }

    @SuppressWarnings("unchecked")
    public static <T extends WalServer> T boot(String[] args) {
        String proto = PROTO;

        for (int i = 0, n = args.length; i < n; ++i) {
            String arg = args[i];
            if ("--proto".equals(arg)) {
                proto = args[++i];
            }
        }

        if ("rmi".equals(proto)) {
            return (T)new RmiWalServer().start(args);
        }

        throw new IllegalArgumentException("Unknown wal server proto: " + proto);
    }

    protected String host = HOST;
    protected int port = PORT;
    protected String dataDir = DIR;
    protected String name = NAME;
    protected String user = USER;
    protected String password = PASSWORD;
    protected int fetchSize = FETCH_SIZE;

    protected String masterURL = MASTER_URL;
    protected String masterUser = MASTER_USER;
    protected String masterPassword = MASTER_PASSWORD;

    protected Waler waler;

    protected WalServer() {

    }

    protected WalServer(String dataDir, String host, int port, String service,
                        String user, String password, int fetchSize,
                        String masterURL, String masterUser, String masterPassword) {
        this.dataDir = dataDir;
        this.host = host;
        this.port = port;
        this.name = service;
        this.user = user;
        this.password = password;
        this.fetchSize = fetchSize;
        this.masterURL = masterURL;
        this.masterUser = masterUser;
        this.masterPassword = masterPassword;
    }

    public synchronized WalServer start(String[] args) throws IllegalStateException, WalException {
        if (isOpen()) {
            throw new IllegalStateException("Wal server started");
        }

        for (int i = 0, n = args.length; i < n; ++i) {
            String arg = args[i];
            if ("-h".equals(arg) || "--host".equals(arg)) {
                this.host = args[++i];
            } else if ("-P".equals(arg) || "--port".equals(arg)) {
                this.port = Integer.decode(args[++i]);
            } else if ("-d".equals(arg) || "--data-dir".equals(arg)) {
                this.dataDir = args[++i];
            } else if ("-s".equals(arg) || "--service".equals(arg)) {
                this.name = args[++i];
            } else if ("-u".equals(arg) || "--user".equals(arg)) {
                this.user = args[++i];
            } else if ("-p".equals(arg) || "--password".equals(arg)) {
                this.password = args[++i];
            } else if ("--fetch-size".equals(arg)) {
                this.fetchSize = Integer.decode(args[++i]);
            } else if ("--master-url".equals(arg)) {
                this.masterURL = args[++i];
            } else if ("--master-user".equals(arg)) {
                this.masterUser = args[++i];
            } else if ("--master-password".equals(arg)) {
                this.masterPassword = args[++i];
            }
        }

        // Do open wal
        this.waler = open(this.dataDir, this.fetchSize, this.masterURL,
                this.masterUser, this.masterPassword);

        return this;
    }

    protected Waler open(String dataDir, int fetchSize, String masterURL,
                         String masterUser, String masterPassword) throws WalException {
        if (masterURL == null) {
            // Master server
            return WalerFactory.open(dataDir, fetchSize, true);
        } else {
            // Slave server
            Properties info = new Properties();
            info.put("dataDir", dataDir);
            info.put("fetchSize", fetchSize + "");
            info.put("fetchLast", true + "");
            if (masterUser != null) info.put("user", masterUser);
            if (masterPassword != null) info.put("password", masterPassword);
            return WalDriverManager.connect(masterURL, info);
        }
    }

    public Waler getWaler() {
        return this.waler;
    }

    public boolean isOpen() {
        Waler waler = this.waler;
        return (waler != null && waler.isOpen());
    }

    @Override
    public void close() {
        IoUtils.close(this.waler);
    }

}
