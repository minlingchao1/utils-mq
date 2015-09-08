package com.diwayou.utils.mq.message.codec;

import com.alibaba.fastjson.JSON;
import com.diwayou.utils.mq.message.MessageDecoder;

/**
 * Created by cn40387 on 15/5/13.
 *
 * Because we need to parse the original data from byte array message, we must create a new
 * JsonMessageDecoder for every T.
 */
public class JsonMessageDecoder<T> implements MessageDecoder<T> {

    private Class<T> resultClass;

    public JsonMessageDecoder(Class<T> resultClass) {
        this.resultClass = resultClass;
    }

    @Override
    public T decode(byte[] message) {
        return JSON.parseObject(message, resultClass);
    }
}
