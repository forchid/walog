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

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.Comparator;

/**
 * @author little-pan
 * @since 2019-12-22
 *
 */
public final class LogFileUtils {
    
    private static final char[] HEX = {
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    
    private LogFileUtils() {
        // NOOP
    }
    
    public static long lsn(String filename) {
        final int len = filename.length();
        
        if (len != 16) {
            throw new IllegalArgumentException("filename length " + len);
        }
        
        long lsn = 0;
        for (int i = 0, n = len >> 1; i < n; ) {
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
        char[] hex = new char[16];
        for (int i = 0, j = 64, n = hex.length; i < n; ) {
            byte k = (byte)(lsn >>> (j -= 8));
            hex[i++] = HEX[(k >> 4) & 0x0f];
            hex[i++] = HEX[(k & 0x0f)];
        }
        
        return new String(hex);
    }
    
    public static File[] listFiles(File dir) {
        return listFiles(dir, false);
    }
    
    public static File[] listFiles(File dir, boolean asc) {
        File[] logFiles = dir.listFiles(new LogFileFilter());
        Arrays.sort(logFiles, new LogFileSorter(asc));
        return logFiles;
    }
    
    public static File lastFile(File dir) {
        File[] logFiles = listFiles(dir, false);
        if (logFiles.length > 0) {
            return logFiles[0];
        }
        
        return null;
    }
    
    public static File firstFile(File dir) {
        File[] logFiles = listFiles(dir, true);
        if (logFiles.length > 0) {
            return logFiles[0];
        }
        
        return null;
    }
    
    static class LogFileFilter implements FileFilter {
        
        @Override
        public boolean accept(File file) {
            if (!file.isFile()) {
                return false;
            }
            
            final String name = file.getName();
            final int length = name.length();
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
    
    static class LogFileSorter implements Comparator<File> {
        
        final boolean asc;
        
        public LogFileSorter() {
            this(false);
        }
        
        public LogFileSorter(boolean asc) {
            this.asc = asc;
        }

        @Override
        public int compare(File a, File b) {
            int i = a.getName().compareTo(b.getName());
            return (this.asc? i: -i);
        }
        
    }

}
