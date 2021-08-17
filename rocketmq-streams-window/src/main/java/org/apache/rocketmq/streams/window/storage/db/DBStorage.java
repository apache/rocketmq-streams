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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.AbstractWindowStorage;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

/**
 * database storage
 */
public class DBStorage<T extends WindowBaseValue> extends AbstractWindowStorage<T> {

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

    @Override
    public void removeKeys(Collection<String> keys) {

    }

    @Override
    public WindowBaseValueIterator<T> loadWindowInstanceSplitData(String localStorePrefix, String queueId, String windowInstanceId, String keyPrex, Class<T> clazz) {

        //search max partition number in case of inserting fresh data [min,max)
        long maxPartitionIndex = getPartitionNum(queueId, windowInstanceId, clazz, true) + 1;
        long mimPartitionIndex = getPartitionNum(queueId, windowInstanceId, clazz, false) - 1;
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

    @Override
    public void clearCache(ISplit channelQueue, Class<T> clazz) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public void delete(String windowInstanceId, Set<String> queueIds, Class<T> clazz) {
        String sql = "delete from " + ORMUtil.getTableName(clazz) + " where window_instance_id = '" + StringUtil.createMD5Str(windowInstanceId) + "'";
        ORMUtil.executeSQL(
            sql,
            new HashMap<>(4));
    }

    public static class DBIterator<T extends WindowBaseValue> extends WindowBaseValueIterator<T> {
        private LinkedList<T> container = new LinkedList<>();
        int batchSize = 1000;
        private boolean exist = true;

        private long maxPartitionIndex;
        private Class<T> clazz;

        String sql;

        public DBIterator(String queueId, String windowInstanceId, String keyPrex, Class<T> clazz, long maxPartitionIndex) {
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

    protected Long getPartitionNum(String queueId, String windowInstanceId, Class<T> clazz, boolean isMax) {
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

}
