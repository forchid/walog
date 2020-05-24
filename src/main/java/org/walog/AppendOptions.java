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

package org.walog;

import static java.lang.Integer.getInteger;

public class AppendOptions {

    public static int QUEUE_SIZE     = getInteger("org.walog.append.queueSize", 128);
    public static int BATCH_SIZE     = getInteger("org.walog.append.batchSize", 128);
    public static int APPEND_TIMEOUT = getInteger("org.walog.append.timeout", 50000);
    public static int AUTO_FLUSH     = getInteger("org.walog.append.autoFlush", 1);
    public static int FLUSH_PERIOD   = getInteger("org.walog.append.flushPeriod", 100);
    public static int FLUSH_UNLOCK   = getInteger("org.walog.append.flushUnlock", 1);
    public static int ASYNC_MODE     = getInteger("org.walog.append.asyncMode", 1);

    private boolean flushUnlock = FLUSH_UNLOCK == 1;
    private int asyncMode = ASYNC_MODE;
    private int queueSize = QUEUE_SIZE;
    private int batchSize = BATCH_SIZE;

    private AppendOptions() {

    }

    public boolean isFlushUnlock() {
        return flushUnlock;
    }

    public int getAsyncMode() {
        return asyncMode;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private final AppendOptions source = new AppendOptions();

        public Builder flushUnlock(boolean flushUnlock) {
            this.source.flushUnlock = flushUnlock;
            return this;
        }

        public Builder asyncMode(int asyncMode) {
            this.source.asyncMode = asyncMode;
            return this;
        }

        public Builder queueSize(int queueSize) {
            this.source.queueSize = queueSize;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.source.batchSize = batchSize;
            return this;
        }

        public AppendOptions build() {
            AppendOptions options = new AppendOptions();
            options.flushUnlock = this.source.flushUnlock;
            options.asyncMode = this.source.asyncMode;
            options.queueSize = this.source.queueSize;
            options.batchSize = this.source.batchSize;
            return options;
        }
    }

}
