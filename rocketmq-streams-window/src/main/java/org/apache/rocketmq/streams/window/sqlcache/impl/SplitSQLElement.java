package org.apache.rocketmq.streams.window.sqlcache.impl;

import org.apache.rocketmq.streams.window.sqlcache.ISQLElement;

public class SplitSQLElement implements ISQLElement {
    protected String queueId;
    protected String sql;
    public SplitSQLElement(String splitId,String sql){
        this.queueId=splitId;
        this.sql=sql;
    }


    @Override public boolean isWindowInstanceSQL() {
        return false;
    }

    @Override public boolean isSplitSQL() {
        return true;
    }

    @Override public boolean isFireNotify() {
        return false;
    }

    @Override public String getQueueId() {
        return queueId;
    }

    @Override public String getWindowInstanceId() {
        throw new RuntimeException("can not support this method");
    }

    @Override public String getSQL() {
        return sql;
    }

    @Override public Integer getIndex() {
        return null;
    }

    @Override
    public void setIndex(int index) {

    }
}
