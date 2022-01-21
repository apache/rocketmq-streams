package org.apache.rocketmq.streams.window.storage;
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

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.store.kv.KeyValuePair;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class RemoteStorage implements IStorage {

    private static DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
    private static String namesrv = "120.77.26.21:9876";

    static {
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setNamesrvAddr(namesrv);

        try {
            defaultMQAdminExt.start();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    protected void put(String key, Object value) {
        if (StringUtil.isEmpty(key) || value == null) {
            return;
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        byte[] jsonBytes = JSON.toJSONBytes(value);

        try {
            defaultMQAdminExt.putKeyValueToStore(keyBytes, jsonBytes);
        } catch (Throwable t) {
            throw new RuntimeException("put key:[" + key + "], value:[" + value + "], to remote error", t);
        }
    }


    protected  <T> T get(String key, Class<T> clazz) {
        if (StringUtil.isEmpty(key)) {
            return null;
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        KeyValuePair keyValuePair;
        try {
            keyValuePair = defaultMQAdminExt.getValueByKeyFromStore(keyBytes);
        } catch (Throwable t) {
            throw new RuntimeException("get value from remote by key:[" + key + "] error", t);
        }

        if (keyValuePair == null || keyValuePair.getValue() == null) {
            return null;
        }

        return JSON.parseObject(keyValuePair.getValue(), clazz);
    }


    public void putWindowInstanceIdAndMsgKey(String windowInstanceId, List<String> msgKeys) {
        if (msgKeys == null || msgKeys.size() == 0 || StringUtil.isEmpty(windowInstanceId)) {
            return;
        }

        this.put(windowInstanceId, unionStoreKeys(msgKeys));
    }


    public List<String> getMsgKeys(String windowInstanceId) {
        if (StringUtil.isEmpty(windowInstanceId)) {
            return null;
        }

        String result = this.get(windowInstanceId, String.class);
        return splitStoreKeys(result);
    }


    private List<String> splitStoreKeys(String unionStoreKeys) {
        if (StringUtil.isEmpty(unionStoreKeys)) {
            return null;
        }

        String[] storeKeys = unionStoreKeys.split(SEPARATOR);

        return Arrays.asList(storeKeys);
    }

    private String unionStoreKeys(Collection<String> storeKeys) {
        if (storeKeys == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String storeKey : storeKeys) {
            sb.append(storeKey);
            sb.append(SEPARATOR);
        }
        return sb.substring(0, sb.lastIndexOf(SEPARATOR));
    }


    @Override
    public void init() {

    }

    @Override
    public void start() {

    }

    @Override
    public void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance) {

    }

    @Override
    public List<WindowInstance> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName) {
        return null;
    }

    @Override
    public void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey) {

    }

    @Override
    public void putWindowBaseValue(String shuffleId, String windowInstanceId,
                                   WindowType windowType, WindowJoinType joinType,
                                   List<WindowBaseValue> windowBaseValue) {

    }

    @Override
    public List<WindowBaseValue> getWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        return null;
    }

    @Override
    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

    }

    @Override
    public String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        return null;
    }

    @Override
    public void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset) {

    }

    @Override
    public void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {

    }

    @Override
    public void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum) {

    }

    @Override
    public Long getMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        return null;
    }

    @Override
    public void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey) {

    }

    @Override
    public int flush(List<String> queueId) {
        return 0;
    }

    @Override
    public void clearCache(String queueId) {

    }

}
