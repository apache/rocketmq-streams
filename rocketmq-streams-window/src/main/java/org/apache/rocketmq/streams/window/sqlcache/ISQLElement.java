package org.apache.rocketmq.streams.window.sqlcache;

import java.util.List;

public interface ISQLElement {

    boolean isWindowInstanceSQL();

    /**
     * window max value for max offset
     * @return
     */
    boolean isSplitSQL();

    /**
     * fire message, can cancel not commit sqls which owned the windowinstance
     * @return
     */
    boolean isFireNotify();


    String getQueueId();


    String getWindowInstanceId();


    String getSQL();


    Integer getIndex();


    void setIndex(int index);
}
