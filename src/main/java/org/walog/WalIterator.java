package org.walog;

public interface WalIterator extends AutoCloseable {

    boolean hasNext();

    Wal next();

    boolean isOpen();

    @Override
    void close();

}
