package com.diwayou.utils.mq.handler;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.diwayou.utils.mq.message.MessageDecoder;

/**
 * Created by cn40387 on 15/5/13.
 */
public interface MessageHandler<T> {

    ConsumeConcurrentlyStatus process(T data, MessageExt message, ConsumeConcurrentlyContext context);

    MessageDecoder<T> getMessageDecoder();
}
