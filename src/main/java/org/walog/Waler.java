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

 /** The WAL manager.
 * 
 * @author little-pan
 * @since 2019-12-22
 *
 */
public interface Waler extends AutoCloseable {
    
    /** Open the logger.
     *
     * @throws WalException if IO error
     */
    void open() throws WalException;
    
    /**
     * Append the log payload to the logger.
     *
     * @return appended log
     * @throws WalException if IO error, appending timeout, or interrupted
     */
    Wal append(byte[] log) throws WalException;

    Wal append(byte[] log, int offset, int length) throws WalException;

    Wal append(String log) throws WalException;

    /** Get current first log in this wal logger.
     *
     * @return the first log, or null if no any log
     * @throws WalException if IO error
     */
    Wal first() throws WalException;

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
    Wal first(long timeout) throws WalException;

    /** Get the log of the specified lsn.
     *
     * @param lsn target log LSN
     * @return the specified log, or null if not found
     * @throws WalException if IO error
     * @throws IllegalArgumentException if the arg lsn is less than 0
     */
    Wal get(long lsn) throws WalException, IllegalArgumentException;

    /** Get the next wal of the specified wal.
     *
     * @param wal the specified wal
     * @return the next wal, or null if not found
     * @throws WalException if IO error
     * @throws IllegalArgumentException if the arg wal lsn is less than 0
     */
    Wal next(Wal wal) throws WalException, IllegalArgumentException;

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
    Wal next(Wal wal, long timeout) throws WalException, IllegalArgumentException;

    /** Iterate wal from the first wal.
     *
     * @return wal iterator
     */
    WalIterator iterator();

    /** Iterate wal from the given lsn.
     *
     * @param lsn the start lsn of iterator
     * @return wal iterator
     * @throws IllegalArgumentException if the arg wal lsn is less than 0
     */
    WalIterator iterator(long lsn) throws IllegalArgumentException;

    WalIterator iterator(long lsn, long timeout) throws IllegalArgumentException;

    boolean purgeTo(String filename) throws WalException;

    boolean purgeTo(long fileLsn) throws WalException;

    boolean clear() throws WalException;

    void sync() throws WalException;

     /** Fetch the last wal.
      *
      * @return the last wal, or null if not found
      * @throws WalException if IO error, timeout or interrupted
      */
    Wal last() throws WalException;
    
    boolean isOpen();
    
    @Override
    void close();
    
}
