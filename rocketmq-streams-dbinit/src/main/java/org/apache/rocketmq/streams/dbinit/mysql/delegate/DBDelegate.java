package org.apache.rocketmq.streams.dbinit.mysql.delegate;

public interface DBDelegate {

    public void init(String driver, String url, String userName,
                              String password);

    public void init();
}
