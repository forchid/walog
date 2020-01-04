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

import java.io.IOException;
import java.util.Iterator;

/** The WAL manager.
 * 
 * @author little-pan
 * @since 2019-12-22
 *
 */
public interface Waler extends AutoCloseable {
    
    /** Open the logger.
     *
     * @throws java.io.IOException if IO error
     */
    void open() throws IOException;
    
    /**
     * Append the log payload to the logger.
     *
     * @return appended log if success, null if appending timeout, or interrupted
     * @throws java.io.IOException if IO error
     */
    Wal append(byte[] payload) throws IOException;

    /** Get current first log in this wal logger.
     *
     * @return the first log, or null if no any log
     * @throws IOException if IO error
     */
    Wal first() throws IOException;

    /** Get the log of the specified lsn.
     *
     * @param lsn target log LSN
     * @return the specified log, or null if not found
     * @throws IOException if IO error
     */
    Wal get(long lsn) throws IOException;

    Iterator<Wal> iterator(long lsn);
    
    void purgeTo(String walFile) throws IOException;

    void purgeTo(long fileLsn) throws IOException;

    void clear() throws IOException;

    void sync() throws IOException;
    
    boolean isOpen();
    
    @Override
    void close();
    
}
