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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.*;

public class LruCache<K, V extends AutoCloseable> implements AutoCloseable {

    private final LinkedHashMap<K, V> cache;
    private volatile boolean open;

    public LruCache() {
        this(16);
    }

    public LruCache(final int capacity){
        this.cache = new LinkedHashMap<K, V>(capacity,0.75f,true){
            @Override
            protected boolean removeEldestEntry(Entry<K, V> eldest) {
                final boolean canRemoved = size() > capacity;
                if (canRemoved) {
                    IoUtils.close(eldest.getValue());
                }
                return canRemoved;
            }
        };
        this.open = true;
    }

    public V get(K key) {
        checkOpen();
        synchronized (this.cache) {
            return this.cache.get(key);
        }
    }

    public V put(K key, V value) {
        checkOpen();
        final V old;
        synchronized (this.cache) {
            old = this.cache.put(key, value);
        }
        IoUtils.close(old);
        return old;
    }

    public V remove(K key) {
        checkOpen();
        final V old;
        synchronized (this.cache) {
            old = this.cache.remove(key);
        }
        IoUtils.close(old);
        return old;
    }

    public void clear() {
        synchronized (this.cache) {
            final Iterator<Entry<K, V>> i = this.cache.entrySet().iterator();
            for(; i.hasNext(); ) {
                IoUtils.close(i.next().getValue());
                i.remove();
            }
        }
    }

    private void checkOpen() {
        if (!isOpen()) throw new IllegalStateException("Cache closed");
    }

    public boolean isOpen() {
        return this.open;
    }

    @Override
    public void close() {
        if (!isOpen()) {
            return;
        }

        clear();
        this.open = false;
    }

}
