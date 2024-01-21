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
package org.apache.rocketmq.streams.window.storage.db;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.AbstractWindowStorage;
import org.apache.rocketmq.streams.window.storage.IRemoteStorage;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

/**
 * database storage
 */
public class DBStorage<T extends WindowBaseValue> extends AbstractWindowStorage<T> implements IRemoteStorage<T> {

    @Override public String multiPutSQL(Map<String, T> values) {
        if (CollectionUtil.isEmpty(values)) {
            return null;
        }
        String sql = ORMUtil.createBatchReplaceSQL(new ArrayList<>(values.values()));
        return sql;
    }

    @Override public String multiPutListSQL(Map<String, List<T>> infoMap) {
        if (CollectionUtil.isNotEmpty(infoMap)) {
            List<T> valueList = duplicate(infoMap);
            return ORMUtil.createBatchReplaceSQL(valueList);
        }
        return null;
    }

    /**
     * the list value has the same store key, add suffix for session window
     *
     * @param infoMap
     * @return
     */
    private List<T> duplicate(Map<String, List<T>> infoMap) {
        List<T> resultList = new ArrayList<>();
        Iterator<Map.Entry<String, List<T>>> iterator = infoMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<T>> entry = iterator.next();
            List<T> valueList = entry.getValue();
            for (int index = 0; index < valueList.size(); index++) {
                //TODO 是否要进行clone
                T value = valueList.get(index);
                value.setMsgKey(value.getMsgKey() + "_" + index);
                resultList.add(value);
            }
        }
        return resultList;
    }

    @Override
    public void multiPut(Map<String, T> values) {
        if (CollectionUtil.isEmpty(values)) {
            return;
        }
        ORMUtil.batchReplaceInto(values.values());
    }

    @Override
    public Map<String, T> multiGet(Class<T> clazz, List<String> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return new HashMap<>(4);
        }
        Map<String, String> md5Key2Keys = new HashMap<>();
        List<String> md5Keys = new ArrayList<>();
        for (String key : keys) {
            String md5Key = StringUtil.createMD5Str(key);
            md5Keys.add(md5Key);
            md5Key2Keys.put(md5Key, key);
        }
        List<T> values = ORMUtil.queryForList("select * from " + ORMUtil.getTableName(clazz) +
            " where msg_key in (" + SQLUtil.createInSql(md5Keys) + " )", new HashMap<>(4), clazz);
        Map<String, T> map = new HashMap<>(keys.size());
        for (T value : values) {
            String key = md5Key2Keys.get(value.getMsgKey());
            map.put(key, value);
        }
        return map;
    }

    @Override public void multiPutList(Map<String, List<T>> elements) {
        if (CollectionUtil.isEmpty(elements)) {
            return;
        }
        List<T> valueList = duplicate(elements);
        ORMUtil.batchReplaceInto(valueList);
    }

    /**
     * the key in db is md5(key)_index
     *
     * @param clazz
     * @param keys
     * @return
     */
    @Override public Map<String, List<T>> multiGetList(Class<T> clazz, List<String> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return new HashMap<>(4);
        }
        Map<String, String> recordMap = new HashMap<>(keys.size());
        List<String> dbKeyList = new ArrayList<>(keys.size());
        List<Pair<String, String>> variableAndValue = new ArrayList<>(keys.size());
        for (String key : keys) {
            String md5Value = StringUtil.createMD5Str(key);
            dbKeyList.add(md5Value);
            recordMap.put(md5Value, key);
            variableAndValue.add(Pair.of("msg_key", md5Value + "%"));
        }
        List<T> values = ORMUtil.queryForList("select * from " + ORMUtil.getTableName(clazz) +
            " where " + SQLUtil.createLikeSql(variableAndValue), new HashMap<>(4), clazz);
        Map<String, List<T>> resultMap = new HashMap<>(keys.size());
        for (T value : values) {
            String dbKeyWithoutSuffix = value.getMsgKey().substring(0, 24);
            value.setMsgKey(dbKeyWithoutSuffix);
            String key = recordMap.get(dbKeyWithoutSuffix);
            List<T> valueList = resultMap.getOrDefault(key, null);
            if (valueList == null) {
                valueList = new ArrayList<>();
                resultMap.put(key, valueList);
            }
            valueList.add(value);
        }
        return resultMap;
    }

    @Override
    public void removeKeys(Collection<String> keys) {

    }

    @Override
    public WindowBaseValueIterator<T> loadWindowInstanceSplitData(String localStorePrefix, String queueId,
        String windowInstanceId, String keyPrex, Class<T> clazz) {

        //search max partition number in case of inserting fresh data [min,max)
        long maxPartitionIndex = getPartitionNum(windowInstanceId, clazz, true) + 1;
        long mimPartitionIndex = getPartitionNum(windowInstanceId, clazz, false) - 1;
        if (maxPartitionIndex <= 1) {
            return new WindowBaseValueIterator<T>() {
                @Override
                public boolean hasNext() {
                    return false;
                }

                @Override
                public T next() {
                    return null;
                }
            };
        }

        DBIterator dbIterator = new DBIterator<T>(queueId, windowInstanceId, keyPrex, clazz, maxPartitionIndex);
        dbIterator.setPartitionNum(mimPartitionIndex);
        return dbIterator;
    }

    @Override public Long getMaxSplitNum(WindowInstance windowInstance, Class<T> clazz) {
        return getPartitionNum(windowInstance.createWindowInstanceId(), clazz, true);
    }

    @Override
    public void clearCache(ISplit channelQueue, Class<T> clazz) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public void delete(String windowInstanceId, String queueId, Class<T> clazz) {

        ORMUtil.executeSQL(
            deleteSQL(windowInstanceId, queueId, clazz),
            new HashMap<>(4));
    }

    @Override public String deleteSQL(String windowInstanceId, String queueId, Class<T> clazz) {
        String sql = "delete from " + ORMUtil.getTableName(clazz) + " where window_instance_id = '" + StringUtil.createMD5Str(windowInstanceId) + "'";
        return sql;
    }

    protected Long getPartitionNum(String windowInstanceId, Class<T> clazz, boolean isMax) {
        String partitionNumSQL = isMax ? "max(partition_num)" : "min(partition_num)";
        String windowInstancePartitionId = StringUtil.createMD5Str(windowInstanceId);
        String sql = "select " + partitionNumSQL + " as partition_num from " + ORMUtil.getTableName(clazz)
            + " where window_instance_partition_id ='" + windowInstancePartitionId + "'";
        WindowBaseValue windowBaseValue = ORMUtil.queryForObject(sql, new HashMap<>(4), clazz);
        if (windowBaseValue == null) {
            return null;
        }
        return windowBaseValue.getPartitionNum();
    }

    public static class DBIterator<T extends WindowBaseValue> extends WindowBaseValueIterator<T> {
        int batchSize = 1000;
        String sql;
        private LinkedList<T> container = new LinkedList<>();
        private boolean exist = true;
        private long maxPartitionIndex;
        private Class<T> clazz;

        public DBIterator(String queueId, String windowInstanceId, String keyPrex, Class<T> clazz,
            long maxPartitionIndex) {
            String windowInstancePartitionId = StringUtil.createMD5Str(windowInstanceId);

            if (StringUtil.isEmpty(keyPrex)) {
                sql = "select * from " + ORMUtil.getTableName(clazz)
                    + " where window_instance_partition_id = '" + windowInstancePartitionId
                    + "' and partition_num > #{partitionNum} order by window_instance_partition_id, partition_num limit "
                    + batchSize;
            } else {
                //join usage(different clazz)
                String prefix = MapKeyUtil.createKey(queueId, windowInstanceId, keyPrex);
                sql = "select * from " + ORMUtil.getTableName(clazz) + " where window_instance_partition_id ='"
                    + windowInstancePartitionId + "' " +
                    "and msg_key like '" + prefix
                    + "%' and  partition_num > #{partitionNum} order by window_instance_partition_id, partition_num  limit "
                    + batchSize;
            }
            this.maxPartitionIndex = maxPartitionIndex;
            this.clazz = clazz;
        }

        @Override
        public boolean hasNext() {
            if (!container.isEmpty()) {
                return true;
            } else if (!exist) {
                return false;
            } else {
                Map<String, Long> parameter = new HashMap<>(4);
                parameter.put("partitionNum", partitionNum);
                exist = partitionNum + batchSize <= maxPartitionIndex;
                List<T> batchResult = ORMUtil.queryForList(sql, parameter, clazz);
                if (CollectionUtil.isEmpty(batchResult)) {
                    return false;
                } else {
                    partitionNum = batchResult.get(batchResult.size() - 1).getPartitionNum();
                    container.addAll(batchResult);
                    return true;
                }
            }
        }

        @Override
        public T next() {
            return container.poll();
        }

    }

}
