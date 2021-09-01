package org.apache.rocketmq.streams.window.sqlcache.impl;

import org.apache.rocketmq.streams.window.sqlcache.ISQLElement;

public class SQLElement implements ISQLElement {
    protected String queueId;
    protected String windowInstanceId;
    protected String sql;
    protected Integer index;
    public SQLElement(String splitId,String windowInstanceId,String sql){
        this.queueId=splitId;
        this.windowInstanceId=windowInstanceId;
        this.sql=sql;
    }

    @Override public boolean isWindowInstanceSQL() {
        return true;
    }

    @Override public boolean isSplitSQL() {
        return false;
    }

    @Override public boolean isFireNotify() {
        return false;
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
        return sql;
    }

    @Override public Integer getIndex() {
        return index;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }


}
