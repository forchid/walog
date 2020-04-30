package org.walog.internal;

public interface Releaseable {

    void retain(int n);

    void retain();

    void release();

}
