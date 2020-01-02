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

package org.walog.util;

import org.walog.Wal;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.lang.Integer.getInteger;

/**
 * @author little-pan
 * @since 2019-12-22
 *
 */
public final class WalFileUtils {

    public static final String EXT = ".wal";
    public static final int ROLL_SIZE;
    static {
        final String name = "org.walog.file.rollSize";
        int n = getInteger(name, 64 << 20);
        if (n > Wal.LSN_OFFSET_MASK) {
            throw new RuntimeException(name+" too big");
        }
        final int minSize = 1 << 20;
        if (n < minSize) {
            throw new RuntimeException(name+" must bigger than or equal to " + minSize);
        }
        ROLL_SIZE = n;
    }
    
    private static final char[] HEX = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

    private static final CopyOnWriteArrayList<File> CACHE = new CopyOnWriteArrayList<>();
    
    private WalFileUtils() {
        // NOOP
    }
    
    public static long lsn(String filename) {
        int len = filename.length();

        // Check filename
        if (len != 16 + EXT.length()) {
            throw new IllegalArgumentException("filename length " + len);
        }
        if (!filename.endsWith(EXT)) {
            throw new IllegalArgumentException("filename " + filename);
        }
        len -= EXT.length();
        
        long lsn = 0;
        int i = 0;
        for (; i < len; ) {
            int a = filename.charAt(i++);
            int b = filename.charAt(i++);
            if (a >= '0' && a <= '9') {
                a -= '0';
            } else if (a >= 'a' && a <= 'f'){
                a = a - 'a' + 10;
            } else {
                throw new IllegalArgumentException("filename " + filename);
            }
            if (b >= '0' && b <= '9') {
                b -= '0';
            } else if (b >= 'a' && b <= 'f'){
                b = b - 'a' + 10;
            } else {
                throw new IllegalArgumentException("filename " + filename);
            }
            lsn <<= 8;
            lsn |= ((a << 4) | b);
        }
        
        return lsn;
    }
    
    public static String filename(long lsn) {
        final char[] hex = new char[16];
        for (int i = 0, j = 64, n = hex.length; i < n; ) {
            byte k = (byte)(lsn >>> (j -= 8));
            hex[i++] = HEX[(k >> 4) & 0x0f];
            hex[i++] = HEX[(k & 0x0f)];
        }
        
        return new String(hex) + EXT;
    }
    
    public static File[] listFiles(File dir) throws  IllegalStateException  {
        return listFiles(dir, false);
    }
    
    public static File[] listFiles(File dir, boolean asc) throws  IllegalStateException {
        final File[] logFiles = dir.listFiles(new WalFileFilter());
        if (logFiles == null) {
            throw new IllegalStateException("Can't list files");
        }
        Arrays.sort(logFiles, new WalFileSorter(asc));
        return logFiles;
    }
    
    public static File lastFile(File dir) throws  IllegalStateException  {
        File[] logFiles = listFiles(dir, false);
        if (logFiles.length > 0) {
            return logFiles[0];
        }
        
        return null;
    }
    
    public static File firstFile(File dir) throws  IllegalStateException {
        File[] logFiles = listFiles(dir, true);
        if (logFiles.length > 0) {
            return logFiles[0];
        }
        
        return null;
    }

    public static File getFile(File dir, long lsn) {
        final String filename = filename(lsn);
        return new File(dir, filename);
    }

    static class WalFileFilter implements FileFilter {
        
        @Override
        public boolean accept(File file) {
            if (!file.isFile()) {
                return false;
            }
            
            final String name = file.getName();
            final int length = name.length() - EXT.length();
            if (length != 16) {
                return false;
            }
            
            for (int i = 0; i < length; ++i) {
                char c = name.charAt(i);
                if (c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
                    continue;
                }
                return false;
            }
            
            return true;
        }
    }
    
    static class WalFileSorter implements Comparator<File> {
        
        final boolean asc;
        
        public WalFileSorter() {
            this(false);
        }
        
        public WalFileSorter(boolean asc) {
            this.asc = asc;
        }

        @Override
        public int compare(File a, File b) {
            final int i = a.getName().compareTo(b.getName());
            return (this.asc? i: -i);
        }
        
    }

}
