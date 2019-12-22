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
public class LoggerFactoryTest extends Test {

    public static void main(String[] args) throws IOException {
        new LoggerFactoryTest().test();
    }

    @Override
    public void test() throws IOException {
        String dir = "loggerFactory";
        File dirFile = getDir(dir);
        
        Logger logger = LoggerFactory.open(dirFile);
        logger.clear();
        logger.close();
        
        logger = LoggerFactory.open(dirFile.getAbsolutePath());
        logger.clear();
        logger.close();
    }

}
