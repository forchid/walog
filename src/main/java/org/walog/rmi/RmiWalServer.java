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

import org.walog.Waler;
import org.walog.WalerFactory;
import org.walog.util.IoUtils;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import static java.lang.Integer.*;
import static java.lang.System.*;

public class RmiWalServer extends UnicastRemoteObject implements RmiWalService, AutoCloseable {

    static final String HOST = getProperty("org.walog.rmi.server.host", "localhost");
    static final int PORT = getInteger("org.walog.rmi.server.port", 1099);
    static final String DIR  = getProperty("org.walog.rmi.server.dataDir", "data");
    static final String NAME = getProperty("org.walog.rmi.server.service", "wal");
    static final String USER = getProperty("org.walog.rmi.server.user");
    static final String PASSWORD = getProperty("org.walog.rmi.server.password");
    static final int FETCH_SIZE = getInteger("org.walog.rmi.server.fetchSize", 64);

    public static void main(String[] args) throws Exception {
        start(args);
    }

    public static RmiWalServer start(String[] args)
            throws RemoteException, MalformedURLException {
        String host = HOST, dataDir = DIR, name = NAME, user = USER, password = PASSWORD;
        int port = PORT, fetchSize = FETCH_SIZE;

        for (int i = 0, n = args.length; i < n; ++i) {
            String arg = args[i];
            if ("-h".equals(arg) || "--host".equals(arg)) {
                host = args[++i];
            } else if ("-P".equals(arg) || "--port".equals(arg)) {
                port = Integer.decode(args[++i]);
            } else if ("-d".equals(arg) || "--data-dir".equals(arg)) {
                dataDir = args[++i];
            } else if ("-s".equals(arg) || "--service".equals(arg)) {
                name = args[++i];
            } else if ("-u".equals(arg) || "--user".equals(arg)) {
                user = args[++i];
            } else if ("-p".equals(arg) || "--password".equals(arg)) {
                password = args[++i];
            } else if ("--fetch-size".equals(arg)) {
                fetchSize = Integer.decode(args[++i]);
            }
        }

        RmiWalServer service = new RmiWalServer(dataDir, host, port, name, user, password, fetchSize);
        boolean failed = true;
        try {
            port = service.port;
            try {

                LocateRegistry.createRegistry(port);
            } catch (RemoteException e) {
                // Ignore
            }

            service.rmiURL = String.format("rmi://%s:%d/%s", service.host, port, service.name);
            Naming.rebind(service.rmiURL, service);
            service.bound = true;
            failed = false;
            return service;
        } finally {
            if(failed) {
                IoUtils.close(service);
            }
        }
    }

    protected final String host;
    protected final int port;
    protected final String dataDir;
    protected final String name;
    protected final String user;
    protected final String password;
    protected final int fetchSize;

    protected Waler waler;
    protected String rmiURL;
    protected boolean bound;

    public RmiWalServer(String dataDir) throws RemoteException {
        this(dataDir, HOST, PORT, NAME, USER, PASSWORD, FETCH_SIZE);
    }

    public RmiWalServer(String dataDir, String host, int port) throws RemoteException {
        this(dataDir, host, port, NAME, USER, PASSWORD, FETCH_SIZE);
    }

    public RmiWalServer(String dataDir, String host, int port, String service,
                        String user, String password) throws RemoteException {
        this(dataDir, host, port, service, user, password, FETCH_SIZE);
    }

    public RmiWalServer(String dataDir, String host, int port, String service,
                        String user, String password, int fetchSize) throws RemoteException {
        this.host = host;
        this.port = port;
        this.name = service;
        this.user = user;
        this.password = password;
        this.dataDir = dataDir;
        this.fetchSize = fetchSize;
        this.waler = open(dataDir, fetchSize);
    }

    static Waler open(String dataDir, int fetchSize) {
        return WalerFactory.open(dataDir, fetchSize, true);
    }

    @Override
    public RmiWrapper connect(Properties info) throws RemoteException {
        if (this.user != null || this.password != null) {
            if (info == null) {
                // 0) Auth failed
                return null;
            }
            String user = info.getProperty("user");
            String password = info.getProperty("password");
            if (this.user != null && !this.user.equalsIgnoreCase(user)) {
                // 1) Auth failed
                return null;
            }
            if (this.password != null && !this.password.equals(password)) {
                // 2) Auth failed
                return null;
            }
        }

        return new WalerWrapper(this.waler);
    }

    @Override
    public void close() {
        IoUtils.close(this.waler);
        if (this.bound) {
            try {
                Naming.unbind(this.rmiURL);
            } catch (Exception e) {
                // Ignore
            }
        }
    }

}
