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

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Properties;

import static java.lang.Integer.*;
import static java.lang.System.*;

/** A master/slave wal server based on rmi protocol.
 */
public class RmiWalServer extends WalServer implements RmiWalService {

    static final String PROP_PREFIX = "org.walog.rmi.server";

    static final String HOST = getProperty(PROP_PREFIX+".host", WalServer.HOST);
    static final int PORT = getInteger(PROP_PREFIX+".port", WalServer.PORT);
    static final String DIR  = getProperty(PROP_PREFIX+".dataDir", WalServer.DIR);
    static final String NAME = getProperty(PROP_PREFIX+".service", "wal");
    static final String USER = getProperty(PROP_PREFIX+".user", WalServer.USER);
    static final String PASSWORD = getProperty(PROP_PREFIX+".password", WalServer.PASSWORD);
    static final int FETCH_SIZE = getInteger(PROP_PREFIX+".fetchSize", WalServer.FETCH_SIZE);
    // Master config
    static final String MASTER_URL = getProperty(PROP_PREFIX+".master.url", WalServer.MASTER_URL);
    static final String MASTER_USER = getProperty(PROP_PREFIX+".master.user", WalServer.MASTER_USER);
    static final String MASTER_PASSWORD = getProperty(PROP_PREFIX+".master.password", WalServer.MASTER_PASSWORD);

    public static void main(String[] args) {
        new RmiWalServer().start(args);
    }

    protected String rmiURL;
    protected boolean bound;

    public RmiWalServer() {
        this(DIR);
    }

    public RmiWalServer(String dataDir) {
        this(dataDir, HOST, PORT, NAME, USER, PASSWORD, FETCH_SIZE, MASTER_URL, MASTER_USER, MASTER_PASSWORD);
    }

    public RmiWalServer(String dataDir, String host, int port) {
        this(dataDir, host, port, NAME, USER, PASSWORD, FETCH_SIZE, MASTER_URL, MASTER_USER, MASTER_PASSWORD);
    }

    public RmiWalServer(String dataDir, String host, int port, String service,
                        String user, String password) {
        this(dataDir, host, port, service, user, password, FETCH_SIZE, MASTER_URL, MASTER_USER, MASTER_PASSWORD);
    }

    public RmiWalServer(String dataDir, String host, int port, String service,
                        String user, String password, int fetchSize) {
        this(dataDir, host, port, service, user, password, fetchSize, MASTER_URL, MASTER_USER, MASTER_PASSWORD);
    }

    public RmiWalServer(String dataDir, String host, int port, String service,
                        String user, String password, int fetchSize,
                        String masterURL, String masterUser, String masterPassword) {
        super(dataDir, host, port, service, user, password, fetchSize, masterURL, masterUser, masterPassword);
    }

    @Override
    public RmiWalServer start(String[] args) {
        super.start(args);

        boolean failed = true;
        try {
            int port = this.port;
            try {
                LocateRegistry.createRegistry(port);
            } catch (RemoteException e) {
                // Ignore
            }

            UnicastRemoteObject.exportObject(this, 0);
            this.rmiURL = String.format("rmi://%s:%d/%s", this.host, port, this.name);
            Naming.rebind(this.rmiURL, this);
            this.bound = true;
            failed = false;
            return this;
        } catch (RemoteException | MalformedURLException e) {
            throw new NetWalException("Start rmi wal server failed", e);
        } finally {
            if (failed) {
                close();
            }
        }
    }

    @Override
    public RmiWrapper connect(Properties info) throws RemoteException {
        if (!isOpen()) {
            throw new RemoteException("Wal rmi server not opened");
        }

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
        super.close();

        if (this.bound) {
            try {
                Naming.unbind(this.rmiURL);
            } catch (Exception e) {
                // Ignore
            }
        }
    }

}
