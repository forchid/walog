package org.walog.rmi;

import org.walog.Wal;
import org.walog.WalException;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface RmiWrapper extends Remote, AutoCloseable {

    void open() throws WalException, RemoteException;

    /**
     * Append the log payload to the logger.
     *
     * @return appended log
     * @throws WalException if IO error, appending timeout, or interrupted
     */
    Wal append(byte[] log) throws WalException, RemoteException;

    Wal append(byte[] log, int offset, int length) throws WalException, RemoteException;

    Wal append(String log) throws WalException, RemoteException;

    /** Get current first log in this wal logger.
     *
     * @return the first log, or null if no any log
     * @throws WalException if IO error
     */
    Wal first() throws WalException, RemoteException;

    /** Get current first log in this wal logger.
     *
     * @param timeout the wait timeout millisecond if has reached to the end of the last wal file.
     *                1) The timeout bigger than 0, then waits the specified millisecond, or
     *                there is a wal appended to the last wal file;
     *                2) The timeout equal to 0, simply waits until there is a wal appended to
     *                the last wal file;
     *                3) The timeout less than 0, non-block and simple as first()
     * @return the first log, or null if no any log
     * @throws WalException if IO error, timeout, or interrupted
     */
    Wal first(long timeout) throws WalException, RemoteException;

    /** Get the log of the specified lsn.
     *
     * @param lsn target log LSN
     * @return the specified log, or null if not found
     * @throws WalException if IO error
     * @throws IllegalArgumentException if the arg lsn is less than 0
     */
    Wal get(long lsn) throws WalException, IllegalArgumentException, RemoteException;

    /** Get the next wal of the specified wal.
     *
     * @param wal the specified wal
     * @return the next wal, or null if not found
     * @throws WalException if IO error
     * @throws IllegalArgumentException if the arg wal lsn is less than 0
     */
    Wal next(Wal wal) throws WalException, IllegalArgumentException, RemoteException;

    /** The timeout version fo next(wal).
     *
     * @param wal the specified log
     * @param timeout the wait timeout millisecond if has reached to the end of the last wal file.
     *                1) The timeout bigger than 0, then waits the specified millisecond, or
     *                there is a wal appended to the last wal file;
     *                2) The timeout equal to 0, simply waits until there is a wal appended to
     *                the last wal file;
     *                3) The timeout less than 0, non-block and simple as next(wal)
     * @return the next wal, or null if not found
     * @throws WalException if IO error, timeout has elapsed, or interrupted
     * @throws IllegalArgumentException if the arg wal lsn is less than 0
     */
    Wal next(Wal wal, long timeout)
            throws WalException, IllegalArgumentException, RemoteException;

    /** Iterate wal from the first wal.
     *
     * @return wal iterator
     */
    RmiIteratorWrapper iterator() throws RemoteException;

    /** Iterate wal from the given lsn.
     *
     * @param lsn the start lsn of iterator
     * @return wal iterator
     * @throws IllegalArgumentException if the arg wal lsn is less than 0
     */
    RmiIteratorWrapper iterator(long lsn) throws IllegalArgumentException, RemoteException;

    RmiIteratorWrapper iterator(long lsn, long timeout)
            throws IllegalArgumentException, RemoteException;

    boolean purgeTo(String filename) throws WalException, RemoteException;

    boolean purgeTo(long fileLsn) throws WalException, RemoteException;

    boolean clear() throws WalException, RemoteException;

    void sync() throws WalException, RemoteException;

    /** Fetch the last wal.
     *
     * @return the last wal, or null if not found
     * @throws WalException if IO error, timeout or interrupted
     */
    Wal last() throws WalException, RemoteException;

    boolean isOpen() throws RemoteException;

    @Override
    void close() throws RemoteException;

}
