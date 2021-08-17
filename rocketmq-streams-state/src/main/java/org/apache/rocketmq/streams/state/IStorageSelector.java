package org.apache.rocketmq.streams.state;

public interface IStorageSelector {

    boolean isLocalStorage(String... namespaces);
}
