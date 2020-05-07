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

package org.walog.util;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Proc {

    protected final String mainClass;
    protected String[] props;
    protected final String[] args;

    protected Process proc;
    protected int exitCode = 1;
    protected String workDir;

    public Proc(String mainClass, String[] args) {
        this.mainClass = mainClass;
        this.args = args;
    }

    public void setProperties(String[] props) {
        this.props = props;
    }

    public void setWorkDir(String workDir) {
        this.workDir = workDir;
    }

    public void start() throws IOException {
        List<String> cmd = new ArrayList<>();
        cmd.add("java");
        if (this.props != null) {
            for (String prop: this.props) {
                cmd.add(prop);
            }
        }
        cmd.add(this.mainClass);
        if (this.args != null) {
            for (String arg: this.args) {
                cmd.add(arg);
            }
        }

        ProcessBuilder pb = new ProcessBuilder(cmd);
        if (this.workDir != null) {
            pb.directory(new File(this.workDir));
        }

        this.proc = pb.start();
        Thread stdout = new Thread(new Runnable() {
            @Override
            public void run() {
                InputStream in = proc.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                try {
                    for(;;) {
                        String line = reader.readLine();
                        if (line == null) {
                            return;
                        }
                        System.out.println(line);
                    }
                } catch (IOException e){
                    // Ignore
                }
            }
        }, "proc-stdout");

        Thread stderr = new Thread(new Runnable() {
            @Override
            public void run() {
                InputStream in = proc.getErrorStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                try {
                    for(;;) {
                        String line = reader.readLine();
                        if (line == null) {
                            return;
                        }
                        System.err.println(line);
                    }
                } catch (IOException e){
                    // Ignore
                }
            }
        }, "proc-stderr");

        stdout.start();
        stderr.start();
    }

    public void join() throws InterruptedException {
        if (this.proc != null) {
            this.exitCode = this.proc.waitFor();
        }
    }

    public void check() {
        if (this.exitCode != 0) {
            throw new AssertionError("exitCode " + this.exitCode);
        }
    }

    public void kill() {
        if (this.proc != null) {
            this.proc.destroy();
        }
    }

}
