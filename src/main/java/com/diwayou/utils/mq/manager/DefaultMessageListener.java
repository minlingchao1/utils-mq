package com.diwayou.utils.mq.manager;

import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.diwayou.utils.mq.message.MessageDecoder;
import com.diwayou.utils.mq.handler.MessageHandler;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by cn40387 on 15/5/13.
 */
public class DefaultMessageListener implements MessageListenerConcurrently {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageListener.class);

    private Map<String, Map<String, MessageHandler>> topicToTagHandler;

    public void init() {
        if (MapUtils.isEmpty(topicToTagHandler)) {
            throw new IllegalArgumentException("You must config topicToTagHandlerList in DefaultMessageListener");
        }

        for (Map.Entry<String, Map<String, MessageHandler>> entry : topicToTagHandler.entrySet()) {
            String topic = entry.getKey();
            Map<String, MessageHandler> tagToHandler = entry.getValue();

            if (MapUtils.isEmpty(tagToHandler)) {
                throw new IllegalArgumentException("You must config handler for topic=" + topic);
            }
        }
    }

    @Override
    public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
        for (MessageExt message : msgs) {
            String topic = message.getTopic();

            Map<String, MessageHandler> tagToHandler = topicToTagHandler.get(topic);
            if (MapUtils.isEmpty(tagToHandler)) {
                String errMsg = "Can't find tagToHandler for topic=" + topic;
                logger.error(errMsg);

                throw new RuntimeException(errMsg);
            }

            MessageHandler messageHandler = null;
            String tag = message.getTags();
            if (StringUtils.isEmpty(tag)) {
                if (MapUtils.isEmpty(tagToHandler)) {
                    String errMsg = "Can't find handler for topic=" + topic;
                    logger.error(errMsg);

                    throw new RuntimeException(errMsg);
                } else {
                    if (tagToHandler.size() != 1) {
                        String errMsg = "Message doesn't have tag value but tagToHandler size() > 1,topic=" + topic;
                        logger.error(errMsg);

                        throw new RuntimeException(errMsg);
                    }

                    Map.Entry<String, MessageHandler> defaultHandler = tagToHandler.entrySet().iterator().next();
                    if (!defaultHandler.getKey().equals("*")) {
                        String errMsg = "Message have one handler but tag is not *,topic=" + topic;
                        logger.error(errMsg);

                        throw new RuntimeException(errMsg);
                    }

                    messageHandler = defaultHandler.getValue();
                }
            } else {
                messageHandler = tagToHandler.get(tag);
                if (messageHandler == null) {
                    String errMsg = "Can't find handler for topic=" + topic + " tag=" + tag;
                    logger.error(errMsg);

                    throw new RuntimeException(errMsg);
                }
            }

            MessageDecoder decoder = messageHandler.getMessageDecoder();

            return messageHandler.process(decoder.decode(message.getBody()), message, context);
        }

        throw new RuntimeException("Because consume one message once, code must not execute here.");
    }

    public Map<String, Map<String, MessageHandler>> getTopicToTagHandler() {
        return topicToTagHandler;
    }

    public void setTopicToTagHandler(Map<String, Map<String, MessageHandler>> topicToTagHandler) {
        this.topicToTagHandler = topicToTagHandler;
    }
}
