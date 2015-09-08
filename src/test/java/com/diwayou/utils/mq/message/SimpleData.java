package com.diwayou.utils.mq.message;

/**
 * Created by cn40387 on 15/5/13.
 */
public class SimpleData {

    private String name;

    private String password;

    public SimpleData() {
    }

    public SimpleData(String name, String password) {
        this.name = name;
        this.password = password;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
