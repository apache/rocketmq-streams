package org.apache.rocketmq.streams.window.sqlcache.impl;

import org.apache.rocketmq.streams.window.sqlcache.ISQLElement;

public class FiredNotifySQLElement implements ISQLElement {
    protected String queueId;
    protected String windowInstanceId;
    public FiredNotifySQLElement(String splitId,String windowInstanceId){
        this.queueId=splitId;
        this.windowInstanceId=windowInstanceId;
    }

    @Override public boolean isWindowInstanceSQL() {
        return false;
    }

    @Override public boolean isSplitSQL() {
        return false;
    }

    @Override public boolean isFireNotify() {
        return true;
    }

    @Override
    public String getQueueId() {
        return queueId;
    }

    @Override
    public String getWindowInstanceId() {
        return windowInstanceId;
    }

    @Override
    public String getSQL() {
        throw new RuntimeException("can not support this method");
    }

    @Override public Integer getIndex() {
        return null;
    }

    @Override
    public void setIndex(int index) {

    }
}
