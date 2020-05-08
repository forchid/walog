package org.walog;

public interface Releaseable {

    void retain(int n);

    void retain();

    void release();

}
