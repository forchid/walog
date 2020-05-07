package org.walog;

import java.util.Iterator;

public interface WalIterator extends AutoCloseable, Iterator<Wal> {

    @Override
    boolean hasNext() throws WalException;

    @Override
    Wal next() throws WalException;

    @Override
    void remove();

    boolean isOpen();

    @Override
    void close();

}
