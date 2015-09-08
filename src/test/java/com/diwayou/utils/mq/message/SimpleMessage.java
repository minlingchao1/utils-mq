package com.diwayou.utils.mq.message;

import com.diwayou.utils.mq.message.codec.JsonMessageEncoder;

/**
 * Created by cn40387 on 15/5/13.
 */
public class SimpleMessage extends AbstractMessage<SimpleData> {

    public SimpleMessage(String key, SimpleData data) {
        super("DiwayouTest", "1", key, data);
    }

    @Override
    public MessageEncoder<SimpleData> getMessageEncoder() {
        return JsonMessageEncoder.create();
    }
}
