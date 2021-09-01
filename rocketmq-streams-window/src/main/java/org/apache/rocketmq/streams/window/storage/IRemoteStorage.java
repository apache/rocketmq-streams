package org.apache.rocketmq.streams.window.storage;

import java.util.Map;
import java.util.Set;

public interface IRemoteStorage<T> extends ICommonStorage<T> {

    //多组key value批量存储
    String multiPutSQL(Map<String, T> values);



    String deleteSQL(String windowInstanceId, String queueId, Class<T> clazz);
}
