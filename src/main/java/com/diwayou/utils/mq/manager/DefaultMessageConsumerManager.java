package com.diwayou.utils.mq.manager;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListener;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.google.common.collect.Lists;
import com.diwayou.utils.mq.handler.MessageHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Created by cn40387 on 15/5/12.
 */
public class DefaultMessageConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(DefaultMessageConsumerManager.class);

    private String consumerGroup;

    private String namesrvAddr;

    private String consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET.name();

    private String consumeTimestamp = minutesBeforeToHumanString(30); // 30 minutes

    private int pullThresholdForQueue = 1000;

    private long pullInterval = 0;

    private int pullBatchSize = 32;

    private DefaultMQPushConsumer defaultMQPushConsumer;

    private DefaultMessageListener messageListener;

    public void init() throws MQClientException {
        if (StringUtils.isEmpty(consumerGroup)) {
            throw new IllegalArgumentException("Your must config consumerGroup.");
        }
        if (StringUtils.isEmpty(namesrvAddr)) {
            throw new IllegalArgumentException("You must config namesrvAddr");
        }
        if (messageListener == null) {
            throw new IllegalArgumentException("You must config messageListener.");
        }

        defaultMQPushConsumer = new DefaultMQPushConsumer(consumerGroup);
        defaultMQPushConsumer.setNamesrvAddr(namesrvAddr);
        defaultMQPushConsumer.setConsumeFromWhere(ConsumeFromWhere.valueOf(consumeFromWhere));
        defaultMQPushConsumer.setConsumeTimestamp(consumeTimestamp);
        defaultMQPushConsumer.setPullThresholdForQueue(pullThresholdForQueue);
        defaultMQPushConsumer.setPullInterval(pullInterval);
        defaultMQPushConsumer.setPullBatchSize(pullBatchSize);
        defaultMQPushConsumer.setMessageListener(messageListener);

        for (Map.Entry<String, Map<String, MessageHandler>> entry : messageListener.getTopicToTagHandler().entrySet()) {
            String topic = entry.getKey();
            List<String> tags = Lists.newArrayListWithCapacity(entry.getValue().size());
            tags.addAll(entry.getValue().keySet());

            if (CollectionUtils.isEmpty(tags) || (tags.size() == 1 && tags.get(0).equals("*"))) {
                defaultMQPushConsumer.subscribe(topic, "*");

                if (logger.isInfoEnabled()) {
                    logger.info("Subscribe topic={},pattern={}", topic, "*");
                }
            } else {
                StringBuilder tagPattern = new StringBuilder();
                for (int idx = 0; idx < tags.size() - 1; idx++) {
                    tagPattern.append(tags.get(idx));
                    tagPattern.append(" || ");
                }
                tagPattern.append(tags.get(tags.size() - 1));

                defaultMQPushConsumer.subscribe(topic, tagPattern.toString());

                if (logger.isInfoEnabled()) {
                    logger.info("Subscribe topic={},pattern={}", topic, tagPattern.toString());
                }
            }
        }

        defaultMQPushConsumer.start();

        if (logger.isInfoEnabled()) {
            logger.info("DefaultMessageConsumerManager start success.");
        }
    }

    public void destroy() {
        if (defaultMQPushConsumer != null) {
            defaultMQPushConsumer.shutdown();
        }
    }

    private static String minutesBeforeToHumanString(int minutesBefore) {
        if (minutesBefore <= 0) {
            throw new IllegalArgumentException("minutesBefore must > 0");
        }

        return UtilAll.timeMillisToHumanString3(System.currentTimeMillis()
                - (1000 * 60 * minutesBefore));
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(String consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public String getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public void setConsumeFromWhere(String consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public String getConsumeTimestamp() {
        return consumeTimestamp;
    }

    public void setConsumeTimestamp(String consumeTimestamp) {
        this.consumeTimestamp = consumeTimestamp;
    }

    public int getPullThresholdForQueue() {
        return pullThresholdForQueue;
    }

    public void setPullThresholdForQueue(int pullThresholdForQueue) {
        this.pullThresholdForQueue = pullThresholdForQueue;
    }

    public long getPullInterval() {
        return pullInterval;
    }

    public void setPullInterval(long pullInterval) {
        this.pullInterval = pullInterval;
    }

    public int getPullBatchSize() {
        return pullBatchSize;
    }

    public void setPullBatchSize(int pullBatchSize) {
        this.pullBatchSize = pullBatchSize;
    }

    public MessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(DefaultMessageListener messageListener) {
        this.messageListener = messageListener;
    }
}
