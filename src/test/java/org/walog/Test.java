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

import java.io.File;
import java.io.IOException;

/**
 * @author little-pan
 * @since 2019-12-23
 *
 */
public abstract class Test {
    
    protected final static File baseDir = new File(System.getProperty("user.dir"));
    protected final static File testDir = new File(baseDir, "temp");
    
    protected Test() {
        
    }
    
    public void test() throws IOException {
        try {
            prepare();
            doTest();
        } finally {
            cleanup();
        }
    }

    protected void prepare() {
        cleanup();
    }

    protected abstract void doTest() throws IOException;

    protected void cleanup() {}
    
    public static File getDir(String dir) {
        File file = new File(testDir, dir);
        if (!file.isDirectory() && !file.mkdirs()) {
            throw new IllegalStateException("Can't create dir: " + file);
        }

        return file;
    }

    public static void deleteDir(String dir) {
        deleteDir(testDir, dir);
    }

    public static void deleteDir(final File parent, String dir) {
        final File dirFile = new File(parent, dir);
        if (dirFile.isDirectory()) {
            for (final File f: dirFile.listFiles()) {
                if (f.isDirectory()) {
                    deleteDir(dirFile, f.getName());
                } else if (f.isFile() && !f.delete()){
                    throw new IllegalStateException("Can't delete file: " + f);
                }
            }
            if (!dirFile.delete()) {
                throw new IllegalStateException("Can't delete directory: " + dirFile);
            }
        }
    }

}
