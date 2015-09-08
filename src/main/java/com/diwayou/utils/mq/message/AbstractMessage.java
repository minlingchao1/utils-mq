package com.diwayou.utils.mq.message;

import com.alibaba.rocketmq.common.message.Message;
import org.apache.commons.lang.StringUtils;

/**
 * Created by cn40387 on 15/5/12.
 */
public abstract class AbstractMessage<T> {

    private Message message;

    private T data;

    /**
     * Construct a message, call encoder method to encoder the data.
     *
     * @param topic topic can't be null.
     * @param tag you can use tag to route the business logic, in rocketmq tag can be null, but it can't be null here
     * @param key business order id or unique key, you can use this key to find the message in rocketmq console
     * @param data the real data to send, any class can be encoded by message encoder
     */
    public AbstractMessage(String topic, String tag, String key, T data) {
        if (StringUtils.isEmpty(topic)) {
            throw new IllegalArgumentException("topic can't be null or empty.");
        }
        if (StringUtils.isEmpty(tag)) {
            throw new IllegalArgumentException("tag can't be null or empty.");
        }
        if (data == null) {
            throw new IllegalArgumentException("data can't be null.");
        }

        this.data = data;
        this.message = new Message(topic, tag, key, getMessageEncoder().encode(data));
    }

    public abstract MessageEncoder<T> getMessageEncoder();

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getTopic() {
        return this.message.getTopic();
    }

    public String getTag() {
        return this.message.getTags();
    }

    public String getKey() {
        return this.message.getKeys();
    }

    public void putProperty(final String name, final String value) {
        this.message.putUserProperty(name, value);
    }

    public String getProperty(final String name, final String value) {
        return this.message.getUserProperty(name);
    }

    public void setDelayTimeLevel(int level) {
        this.message.setDelayTimeLevel(level);
    }

    public int getDelayTimeLevel() {
        return this.message.getDelayTimeLevel();
    }

    public void setWaitStoreMsgOK(boolean waitStoreMsgOK) {
        this.message.setWaitStoreMsgOK(waitStoreMsgOK);
    }

    public boolean isWaitStoreMsgOK() {
        return this.message.isWaitStoreMsgOK();
    }

    public void setFlag(int flag) {
        this.message.setFlag(flag);
    }

    public int getFlag() {
        return this.message.getFlag();
    }

    public Message getMessage() {
        return message;
    }
}
