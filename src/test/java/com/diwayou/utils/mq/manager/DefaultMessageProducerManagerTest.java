package com.diwayou.utils.mq.manager;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.diwayou.utils.mq.message.SimpleData;
import com.diwayou.utils.mq.message.SimpleMessage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by cn40387 on 15/5/13.
 */
public class DefaultMessageProducerManagerTest {

    private DefaultMessageProducerManager producerManager;

    @Before
    public void init() throws MQClientException {
        producerManager = new DefaultMessageProducerManager();
        producerManager.setProducerGroup("DIWAYOU_TEST");
        producerManager.setNamesrvAddr("10.113.168.132:9876;10.113.169.130:9876");
        producerManager.init();
    }

    @After
    public void destroy() {
        producerManager.destroy();
    }

    @Test
    public void sendSimpleDataTest() {
        SimpleData simpleData = new SimpleData("name", "password");

        for (int i = 0; i < 100; i++) {
            SendResult result = producerManager.sendMessage(new SimpleMessage(String.valueOf(i), simpleData));

            System.out.println("Msg " + i + " " + result.getMsgId());
        }
    }
}
