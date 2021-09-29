package org.apache.rocketmq.streams.state;

import java.util.Map;

public interface ISchama {

    /**
     * split namespace 2 mutil fields, key is field name;value is field Value.
     * use in db storage
     * @param namespace
     * @return
     */
    Map<String,String> parseSchama(String namespace);
}
