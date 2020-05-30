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

import org.walog.util.IoUtils;

import java.util.ArrayList;
import java.util.List;

public class AllTest extends Test {

    protected final List<Test> tests = new ArrayList<>();

    public static void main(String[] args) {
        for (int i = 0; i < iterates; ++i) {
            new AllTest(i).test();
        }
        completed = true;
    }

    public AllTest(int iterate) {
        super(iterate);
    }

    @Override
    protected void doTest() {
        final long start = System.currentTimeMillis();
        for (final Test t: this.tests) {
            final long ts = System.currentTimeMillis();
            IoUtils.info(">> Iterate-%d: %s start", this.iterate, t.getName());
            t.test();
            final long te = System.currentTimeMillis();
            IoUtils.info("<< Iterate-%d: %s end(time %dms)", this.iterate, t.getName(), (te - ts));
        }
        final long end = System.currentTimeMillis();
        IoUtils.info("%s complete(time %dms)", getName(), (end - start));
    }

    @Override
    protected void prepare() {
        final int i = this.iterate;

        add(new CrashTest(i));
        add(new IterateOnAppendTest(i));
        add(new ReplicateTest(i));
        add(new WalerFactoryTest(i));
    }

    @Override
    protected void cleanup() {
        this.tests.clear();
    }

    protected AllTest add(Test t) {
        this.tests.add(t);
        return this;
    }

}
