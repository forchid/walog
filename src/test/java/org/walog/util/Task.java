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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class Task<V> extends Thread {

    static final ThreadLocal<Task<?>> localTask = new ThreadLocal<>();

    protected final AtomicBoolean flag;
    protected final Callable<V> task;
    protected volatile V result;
    protected volatile Throwable cause;
    private volatile boolean completed;

    public Task(Callable<V> task) {
        this(task, null);
    }

    public Task(Callable<V> task, String name) {
        if (name != null) {
            setName(name);
        }
        setDaemon(true);
        this.task = task;
        this.flag = new AtomicBoolean();
    }

    public static Task<?> currentTask() {
        return localTask.get();
    }

    @Override
    public void run() {
        try {
            localTask.set(this);
            this.result = this.task.call();
            this.flag.set(true);
        } catch (final Throwable cause) {
            this.cause = cause;
            this.flag.set(false);
        } finally {
            this.completed = true;
            localTask.remove();
        }
    }

    public void check() {
        if (this.completed && !ok()) {
            throw new AssertionError(getName()+" failed", this.cause);
        }
    }

    public boolean ok() {
        return this.flag.get();
    }

    public V getResult() {
        return this.result;
    }

    public Throwable getCause() {
        return this.cause;
    }

}
