package com.diwayou.utils.mq.handler;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.diwayou.utils.mq.message.MessageDecoder;
import com.diwayou.utils.mq.message.SimpleData;
import com.diwayou.utils.mq.message.codec.JsonMessageDecoder;

/**
 * Created by cn40387 on 15/5/13.
 */
public class SimpleMessageHandler implements MessageHandler<SimpleData> {

    @Override
    public ConsumeConcurrentlyStatus process(SimpleData data, MessageExt message, ConsumeConcurrentlyContext context) {
        System.out.println(data.getName());
        System.out.println(data.getPassword());

        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
    }

    @Override
    public MessageDecoder<SimpleData> getMessageDecoder() {
        return new JsonMessageDecoder<SimpleData>(SimpleData.class);
    }
}
