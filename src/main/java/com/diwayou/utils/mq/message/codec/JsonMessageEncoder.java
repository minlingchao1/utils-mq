package com.diwayou.utils.mq.message.codec;

import com.alibaba.fastjson.JSON;
import com.diwayou.utils.mq.message.MessageEncoder;

/**
 * Created by cn40387 on 15/5/13.
 *
 * Because json encode is stateless, we use singleton to improve performance.
 */
public class JsonMessageEncoder<T> implements MessageEncoder<T> {

    private static final JsonMessageEncoder instance = new JsonMessageEncoder();

    private JsonMessageEncoder() { }

    public static JsonMessageEncoder create() {
        return instance;
    }

    @Override
    public byte[] encode(T message) {
        return JSON.toJSONBytes(message);
    }
}
