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

package org.walog.internal;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class AppendItem<V> {

    public static final int TAG_PAYLOAD = 0x01;
    public static final int TAG_SYNC    = 0x02;
    public static final int TAG_PURGE   = 0x04;
    public static final int TAG_CLEAR   = 0x08;
    public static final int TAG_END     = 0x10;

    public static final Object DUMMY_VALUE = new Object();
    static final AppendItem<?> END_ITEM = new AppendItem<>(TAG_END);

    public final int tag;
    long expiryTime;

    // Simple future
    private final AtomicInteger state;
    private V result;
    private Throwable cause;

    public AppendItem(int tag) {
        this.tag = tag;
        this.state = new AtomicInteger(0);
    }

    public boolean tryRun() {
        // Init -> running
        return this.state.compareAndSet(0, 1);
    }

    public synchronized boolean cancel() {
        final boolean b;
        // Init -> canceled
        if (b = this.state.compareAndSet(0, 4)) {
            this.notifyAll();
        }
        return b;
    }

    public boolean setResult(final Object result) {
        return setResult(result, null);
    }

    public boolean setResult(final Throwable cause) {
        return setResult(null, cause);
    }

    @SuppressWarnings("unchecked")
    public synchronized boolean setResult(Object result, Throwable cause) {
        final boolean b;

        if (cause == null) {
            // Running -> result
            if (b = this.state.compareAndSet(1, 2)) {
                this.result = (V)result;
            }
        } else {
            // Running -> error
            if (b = this.state.compareAndSet(1, 3)) {
                this.cause = cause;
            }
        }

        if (b) this.notifyAll();
        return b;
    }

    public synchronized V get() throws IOException {
        for (;;) {
            try {
                while (!isCompleted()) {
                    this.wait();
                }
                if (this.cause != null) {
                    throw this.cause;
                }
                return this.result;
            } catch (final InterruptedException e) {
                if (this.cancel()) {
                    // Interrupted before appender handles it
                    return null;
                }
                // Ignore after appender handling or handled it
            } catch (final Throwable e) {
                if (e instanceof IOException) {
                    throw (IOException)e;
                } else if (e instanceof RuntimeException){
                    throw (RuntimeException)e;
                } else if (e instanceof Error){
                    throw (Error)e;
                } else {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    public boolean isCompleted() {
        return (this.state.get() >= 2);
    }

}
