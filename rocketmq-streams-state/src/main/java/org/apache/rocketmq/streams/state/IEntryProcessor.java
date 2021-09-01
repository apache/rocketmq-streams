package org.apache.rocketmq.streams.state;

import java.util.Map;

public interface IEntryProcessor<K,V>{

    void processEntry(Map.Entry<K, V> entry);
}
