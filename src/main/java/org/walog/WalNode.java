package org.walog;

import java.nio.ByteBuffer;

/** The abstract of wal master or slave node.
 *
 * @since 2020-05-08 little-pan
 */
public interface WalNode {

    /** Replicate from begin of master wal files */
    long BEGIN_LSN = -1L;

    byte CMD_FETCH_FROM = 0x01;

    byte RES_WAL = 0x01;
    byte RES_ERR = (byte)0xff;

    boolean isOpen();

    /** Allocate a little-endian buffer.
     *
     * @return buffer
     */
    ByteBuffer allocate();

    /** Allocate a specified capacity little-endian buffer.
     *
     * @return buffer
     */
    ByteBuffer allocate(int capacity);

    /**
     * Node connection established.
     */
    void onActive();

    /**
     * Node connection finished.
     */
    void onInactive();

    void send(ByteBuffer buffer) throws WalException;

    void receive(ByteBuffer buffer) throws WalException;

}
