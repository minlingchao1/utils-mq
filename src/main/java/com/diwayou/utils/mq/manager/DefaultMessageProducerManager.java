package com.diwayou.utils.mq.manager;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.diwayou.utils.mq.message.AbstractMessage;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by cn40387 on 15/5/13.
 */
public class DefaultMessageProducerManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageProducerManager.class);

    private static final Logger msgLogger = LoggerFactory.getLogger("msgLogger");

    private String producerGroup;

    private String namesrvAddr;

    private boolean logMessageResult = false;

    private DefaultMQProducer defaultMQProducer;

    public void init() throws MQClientException {
        if (StringUtils.isEmpty(producerGroup)) {
            throw new IllegalArgumentException("You must config producerGroup.");
        }
        if (StringUtils.isEmpty(namesrvAddr)) {
            throw new IllegalArgumentException("You must config namesrvAddr");
        }

        defaultMQProducer = new DefaultMQProducer(producerGroup);
        defaultMQProducer.setNamesrvAddr(namesrvAddr);

        defaultMQProducer.start();

        if (logger.isInfoEnabled()) {
            logger.info("DefaultMessageProducerManager start success.");
        }
    }

    public void destroy() {
        if (defaultMQProducer != null) {
            defaultMQProducer.shutdown();
        }
    }

    public <T> SendResult sendMessage(AbstractMessage<T> message) {
        try {
            SendResult result = defaultMQProducer.send(message.getMessage());

            logMessageResult(message, result);

            return result;
        } catch (Exception e) {
            logger.error("group={} sendMessage error.", producerGroup, e);
            throw new RuntimeException(e);
        }
    }

    public <T> SendResult sendMessage(AbstractMessage<T> message, long timeout) {
        try {
            SendResult result = defaultMQProducer.send(message.getMessage(), timeout);

            logMessageResult(message, result);

            return result;
        } catch (Exception e) {
            logger.error("group={} sendMessage error.", producerGroup, e);
            throw new RuntimeException(e);
        }
    }

    public <T> void sendMessageOneway(AbstractMessage<T> message) {
        try {
            defaultMQProducer.sendOneway(message.getMessage());
        } catch (Exception e) {
            logger.error("group={} sendMessage error.", producerGroup, e);
            throw new RuntimeException(e);
        }
    }

    private <T> void logMessageResult(AbstractMessage<T> message, SendResult result) {
        if (logMessageResult) {
            if (msgLogger != null) {
                msgLogger.error("[topic={},tag={},key={},msgId={}] : data={}", message.getTopic(), message.getTag(),
                        message.getKey(), result.getMsgId(), message.getData());
                return;
            }

            logger.error("[topic={},tag={},key={},msgId={}] : data={}", message.getTopic(), message.getTag(),
                    message.getKey(), result.getMsgId(), message.getData());
        }
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public boolean isLogMessageResult() {
        return logMessageResult;
    }

    public void setLogMessageResult(boolean logMessageResult) {
        this.logMessageResult = logMessageResult;
    }
}
