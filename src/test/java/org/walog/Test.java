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

import org.walog.util.IoUtils;
import org.walog.util.Task;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * @author little-pan
 * @since 2019-12-23
 *
 */
public abstract class Test {
    
    protected final static File baseDir = new File(System.getProperty("user.dir"));
    protected final static File testDir = new File(baseDir, "temp");
    protected static final int iterates = Integer.getInteger("org.walog.test.iterates", 3);

    protected final int iterate;
    
    protected Test(int iterate) {
        this.iterate = iterate;
    }

    protected String getName() {
        return getClass().getSimpleName();
    }
    
    public void test() {
        try {
            prepare();
            doTest();
        } catch (IOException e) {
            throw new RuntimeException(getName()+" failed", e);
        } finally {
            cleanup();
        }
    }

    protected void prepare() {
        cleanup();
        getDir(getName());
    }

    protected abstract void doTest() throws IOException;

    protected void cleanup() {
        deleteDir(getName());
    }

    protected File getDir() {
        return getDir(getName());
    }
    
    public static File getDir(String dir) {
        File file = new File(testDir, dir);
        if (!file.isDirectory()) {
            if (!file.mkdirs()) {
                throw new IllegalStateException("Can't create dir: " + file);
            }
            IoUtils.info("Create directory: %s", file);
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
                    continue;
                }
                if (f.isFile()){
                    if (!f.delete()) {
                        throw new IllegalStateException("Can't delete file: " + f);
                    }
                    IoUtils.info("Delete file: %s", f);
                }
            }
            if (!dirFile.delete()) {
                throw new IllegalStateException("Can't delete directory: " + dirFile);
            }
            IoUtils.info("Delete directory: %s", dirFile);
        }
    }

    protected static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    protected static void join(Thread t) {
        try {
            t.join();
        } catch (InterruptedException e) {
            // Ignore
        }
    }

    protected static <V> Task<V> newTask(Callable<V> callable, String name) {
        return new Task<>(callable, name);
    }

    protected static <V> Task<V> newTask(Callable<V> callable) {
        return new Task<>(callable);
    }

    protected static void fail(String message) throws AssertionError {
        throw new AssertionError(message);
    }

    protected static void asserts(boolean b) throws AssertionError {
        asserts(b, "Asserts failed");
    }

    protected static void asserts(boolean b, String message) throws AssertionError {
        if (b) {
            return;
        }
        throw new AssertionError(message);
    }

    protected static void equals(long a, long b, String message) throws AssertionError {
        if (a == b) {
            return;
        }
        throw new AssertionError(message);
    }

    protected static void equals(double a, double b, String message) throws AssertionError {
        if (a == b) {
            return;
        }
        throw new AssertionError(message);
    }

    protected static void equals(Object a, Object b, String message) throws AssertionError {
        if (a == b) {
            return;
        }
        if (a == null || b == null) {
            throw new AssertionError(message);
        }
        if (!a.equals(b)) {
            throw new AssertionError(message);
        }
    }

}
