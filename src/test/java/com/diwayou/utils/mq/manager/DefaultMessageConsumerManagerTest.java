package com.diwayou.utils.mq.manager;


import com.alibaba.rocketmq.client.exception.MQClientException;
import com.diwayou.utils.mq.handler.MessageHandler;
import com.diwayou.utils.mq.handler.SimpleMessageHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by cn40387 on 15/5/13.
 */
public class DefaultMessageConsumerManagerTest {

    private DefaultMessageConsumerManager consumerManager;

    @Before
    public void init() throws MQClientException {
        consumerManager = new DefaultMessageConsumerManager();
        consumerManager.setNamesrvAddr("10.113.168.132:9876;10.113.169.130:9876");
        consumerManager.setConsumerGroup("DIWAYOU_TEST");

        DefaultMessageListener messageListener = new DefaultMessageListener();
        Map<String, Map<String, MessageHandler>> topicToTagHandler = new HashMap<String, Map<String, MessageHandler>>();
        Map<String, MessageHandler> tagToHandler = new HashMap<String, MessageHandler>();
        tagToHandler.put("1", new SimpleMessageHandler());
        topicToTagHandler.put("DiwayouTest", tagToHandler);
        messageListener.setTopicToTagHandler(topicToTagHandler);
        messageListener.init();
        consumerManager.setMessageListener(messageListener);

        consumerManager.init();
    }

    @After
    public void destroy() {
        consumerManager.destroy();
    }

    @Test
    public void bootstrapTest() throws InterruptedException {
        TimeUnit.HOURS.sleep(1);
    }
}
