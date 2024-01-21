/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.dispatcher.cache;

import java.util.HashMap;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.streams.common.utils.RocketmqUtil;
import org.apache.rocketmq.streams.dispatcher.ICache;

public class RocketmqCache implements ICache {

    private String nameServAddr;

    public RocketmqCache(String nameServAddr) {
        this.nameServAddr = nameServAddr;
    }

    @Override public void putKeyConfig(String namespace, String key, String value) {
        RocketmqUtil.putKeyConfig(namespace, key, value, this.nameServAddr);
    }

    @Override public String getKeyConfig(String namespace, String key) {
        return RocketmqUtil.getKeyConfig(namespace, key, this.nameServAddr);
    }

    @Override public void deleteKeyConfig(String namespace, String key) {
        RocketmqUtil.deleteKeyConfig(namespace, key, this.nameServAddr);
    }

    @Override public HashMap<String, String> getKVListByNameSpace(String namespace) {
        KVTable kvTable = RocketmqUtil.getKVListByNameSpace(namespace, this.nameServAddr);
        assert kvTable != null;
        return kvTable.getTable();
    }

    public String getNameServAddr() {
        return nameServAddr;
    }

    public void setNameServAddr(String nameServAddr) {
        this.nameServAddr = nameServAddr;
    }
}
