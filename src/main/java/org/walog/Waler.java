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

import org.walog.util.WalFileUtils;

import java.io.IOException;
import java.util.Iterator;

import static java.lang.Integer.*;

/** The WAL manager.
 * 
 * @author little-pan
 * @since 2019-12-22
 *
 */
public interface Waler extends AutoCloseable {

    int APPEND_LOCK_TIMEOUT = getInteger("org.walog.append.lockTimeout", 50000);
    int BLOCK_CACHE_SIZE = getInteger("org.walog.block.cacheSize", 16);
    int FILE_ROLL_SIZE = WalFileUtils.ROLL_SIZE;
    
    /** Open the logger.
     * 
     * @return true if open successfully, otherwise false
     * @throws java.io.IOException if IO error
     */
    boolean open() throws IOException;
    
    /**
     * Append the log payload to the logger.
     *
     * @return true if success, false if acquire append lock timeout, or interrupted
     * @throws java.io.IOException if IO error
     */
    boolean append(byte[] payload) throws IOException;
    
    Wal first() throws IOException;
    
    Wal get(long lsn) throws IOException;

    Iterator<Wal> iterator(long lsn);
    
    void purgeTo(String walFile) throws IOException;
    
    boolean clear() throws IOException;
    
    boolean sync() throws IOException;
    
    boolean isOpen();
    
    @Override
    void close();
    
}
