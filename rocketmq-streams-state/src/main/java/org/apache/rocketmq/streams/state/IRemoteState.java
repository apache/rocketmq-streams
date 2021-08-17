package org.apache.rocketmq.streams.state;

import java.util.Iterator;
import java.util.Map;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;

public interface IRemoteState<K,V> extends IState<K,V> {

    void rowIterator(IRowOperator processor);
}
