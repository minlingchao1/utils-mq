package com.diwayou.utils.mq.message;

/**
 * Created by cn40387 on 15/5/12.
 */
public interface MessageDecoder<T> {

    T decode(byte[] message);
}
