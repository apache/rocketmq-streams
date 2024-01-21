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
package org.apache.rocketmq.streams.dispatcher.strategy;

import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.rocketmq.streams.dispatcher.IStrategy;
import org.apache.rocketmq.streams.dispatcher.entity.DispatcherMapper;

public class ConsistencyStrategy implements IStrategy {

    private static String getWorker(SortedMap<Integer, String> sortedMap, String key) {
        //得到该key的hash值
        int hash = getHash(key);
        //得到大于该Hash值的所有Map
        String worker;
        SortedMap<Integer, String> subMap = sortedMap.tailMap(hash);
        if (subMap.isEmpty()) {
            //如果没有比该key的hash值大的，则从第一个node开始
            Integer i = sortedMap.firstKey();
            //返回对应的服务器
            worker = sortedMap.get(i);
        } else {
            //第一个Key就是顺时针过去离node最近的那个结点
            Integer i = subMap.firstKey();
            //返回对应的服务器
            worker = subMap.get(i);
        }
        return worker;
    }

    //使用FNV1_32_HASH算法计算服务器的Hash值
    private static int getHash(String str) {
        final int p = 16777619;
        int hash = (int) 2166136261L;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash ^ str.charAt(i)) * p;
        }
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;

        // 如果算出来的值为负数则取其绝对值
        if (hash < 0) {
            hash = Math.abs(hash);
        }
        return hash;
    }

    @Override public DispatcherMapper dispatch(List<String> tasks, List<String> instances) throws Exception {
        DispatcherMapper dispatcherMapper = new DispatcherMapper();
        SortedMap<Integer, String> sortedMap = new TreeMap<>();
        for (String instanceId : instances) {
            for (int i = 1; i <= 1000; i++) {
                int hash = getHash(instanceId);//nodeName.hashCode();
                sortedMap.put(hash, instanceId);
            }
        }
        for (int i = 0; i < tasks.size(); i++) {
            dispatcherMapper.putTask(getWorker(sortedMap, i + ""), tasks.get(i));
        }
        return dispatcherMapper;
    }
}
