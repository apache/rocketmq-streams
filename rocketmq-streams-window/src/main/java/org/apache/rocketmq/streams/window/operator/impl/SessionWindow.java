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
package org.apache.rocketmq.streams.window.operator.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

/**
 * 实现思路： 1.每个分片一个windowinstance，starttime=2020-12-30:00:00:00 endtime=2999-01-01 01:01:01 2.firetime，第一次创建窗口，firetime=当前时间计算+window size 3.增加存储，按window instance所有groupby的触发时间排序(设计前缀)，每次有数据来时，更新触发时间，触发时间算法如2 4.窗口触发时，检查存储中最近的触发时间是否<=触发时间，如果符合触发条件触发，然后一直迭代到触发时间>当前时间，把最近的触发时间当作window instance的触发时间，修改window instance的firetime 5.清理触发的数据（触发时间<=窗口实例的触发时间）
 */
public class SessionWindow extends WindowOperator {
    private static final String ORDER_BY_FIRE_TIME = "_order_by_fire_time_ ";//key=_order;queueid,windowinstanceid,partitionNum

    @Override
    protected boolean initConfigurable() {
        this.fireMode = 2;
        return super.initConfigurable();
    }

    @Override
    public int fireWindowInstance(WindowInstance instance, String queueId, Map<String, String> queueId2Offset) {
        List<WindowValue> fireWindowValues = new ArrayList<>();
        int fireCount = 0;
        //for(String queueId:queueIds){
        WindowBaseValueIterator<WindowBaseValue> it = storage.loadWindowInstanceSplitData(getOrderBypPrefix(), queueId, instance.createWindowInstanceId(), null, getWindowBaseValueClass());
        if (queueId2Offset != null) {
            String offset = queueId2Offset.get(queueId);
            if (StringUtil.isNotEmpty(offset)) {
                it.setPartitionNum(Long.valueOf(offset));
            }
        }
        boolean hasFinished = true;
        while (it.hasNext()) {
            WindowBaseValue windowBaseValue = it.next();
            if (windowBaseValue == null) {
                continue;
            }
            Date realFireTime = DateUtil.parseTime(instance.getFireTime());
            Long currentMaxTime = instance.getLastMaxUpdateTime();
            Long realFireTimeLong = realFireTime.getTime();
            // System.out.println(DateUtil.format(new Date(currentMaxTime)));
            /**
             * first not fire window value
             */
            if (currentMaxTime - realFireTimeLong < 0) {
                instance.setFireTime(windowBaseValue.getFireTime());
                windowFireSource.registFireWindowInstanceIfNotExist(instance, this);
                hasFinished = false;
                break;
            }
            fireWindowValues.add((WindowValue)windowBaseValue);
            if (fireWindowValues.size() >= windowCache.getBatchSize()) {
                sendFireMessage(fireWindowValues, queueId);
                fireCount += fireWindowValues.size();
                clearWindowValues(fireWindowValues, queueId, instance);
                fireWindowValues = new ArrayList<>();
            }

        }
        if (fireWindowValues.size() > 0) {
            sendFireMessage(fireWindowValues, queueId);
            fireCount += fireWindowValues.size();
            clearWindowValues(fireWindowValues, queueId, instance);
        }
        if (hasFinished) {
            this.windowInstanceMap.remove(instance.createWindowInstanceId());
        }

        //}

        return fireCount;
    }

    @Override
    protected void saveStorage(Map<String, WindowBaseValue> allWindowBasedValue, WindowInstance windowInstance, String queueId) {
        List<String> oldKeys = new ArrayList<>();
        Map<String, WindowBaseValue> partionNumOrders = new HashMap<>();//需要基于key前缀排序partitionnum
        for (WindowBaseValue windowBaseValue : allWindowBasedValue.values()) {
            WindowValue windowValue = (WindowValue)windowBaseValue;
            String oldPartitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()), windowInstance);

            windowBaseValue.setPartitionNum(createPartitionNum((WindowValue)windowBaseValue, queueId, windowInstance));
            windowBaseValue.setFireTime(createSessionFireTime(windowValue.getPartition(), windowValue.getLastUpdateTime()));
            String partitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()), windowInstance);
            if (!partitionNumKey.equals(oldPartitionNumKey)) {
                oldKeys.add(oldPartitionNumKey);
                partionNumOrders.put(partitionNumKey, windowValue);
            }

        }
        this.storage.getLocalStorage().removeKeys(oldKeys);
        storage.multiPut(allWindowBasedValue);
        storage.multiPut(partionNumOrders);
    }

    @Override
    public List<WindowInstance> queryOrCreateWindowInstance(IMessage message, String queueId) {
        Long occurTime = WindowInstance.getOccurTime(this, message);
        Date fireTime = createSessionFireDate(queueId, occurTime);
        WindowInstance windowInstance = this.createWindowInstance("2020-01-01 00:00:00", "2999-01-01 00:00:00", DateUtil.format(fireTime), queueId);
        WindowInstance existWindowInstance = this.getWindowInstanceMap().get(windowInstance.createWindowInstanceId());
        if (existWindowInstance != null) {
            Date windowInstanceFireTime = DateUtil.parse(existWindowInstance.getFireTime());
            boolean hasFired = false;
            while (WindowInstance.getOccurTime(this, message) - windowInstanceFireTime.getTime() > 0) {
                hasFired = true;
                System.out.println(DateUtil.format(new Date(WindowInstance.getOccurTime(this, message))));
                existWindowInstance.setLastMaxUpdateTime(WindowInstance.getOccurTime(this, message));
                this.windowFireSource.executeFireTask(existWindowInstance, true);
                existWindowInstance = this.getWindowInstanceMap().get(windowInstance.createWindowInstanceId());
                if (existWindowInstance == null) {
                    break;
                }
                windowInstanceFireTime = DateUtil.parse(existWindowInstance.getFireTime());

            }
            if (existWindowInstance != null) {
                windowInstance = existWindowInstance;
            }
            if (hasFired) {
                windowInstance.setFireTime(DateUtil.format(fireTime));
            }

        } else {
            windowInstance.setNewWindowInstance(true);
            windowInstance.setFireTime(DateUtil.format(fireTime));
            this.getWindowInstanceMap().put(windowInstance.createWindowInstanceId(), windowInstance);
        }
        List<WindowInstance> windowInstances = new ArrayList<>();
        windowInstances.add(windowInstance);
        return windowInstances;
    }

    /**
     * clear has fired window value
     *
     * @param windowValues
     * @param queueId
     * @param instance
     */
    protected void clearWindowValues(List<WindowValue> windowValues, String queueId, WindowInstance instance) {
        if (windowValues == null || windowValues.size() == 0) {
            return;
        }
        Set<String> deleteKeys = new HashSet<>();
        List<String> msgKeys = new ArrayList<>();
        for (WindowValue windowValue : windowValues) {
            String storeKey = createStoreKey(queueId, windowValue.getGroupBy(), instance);
            String partitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()) + "", instance);
            deleteKeys.add(storeKey);
            deleteKeys.add(partitionNumKey);
            msgKeys.add(windowValue.getMsgKey());
        }
        String sql = "delete from window_value where msg_key in(" + SQLUtil.createInSql(msgKeys) + ")";
        DriverBuilder.createDriver().execute(sql);
        storage.getLocalStorage().removeKeys(deleteKeys);
    }

    protected static String getOrderBypFieldName(WindowValue windowValue) {
        return MapKeyUtil.createKey(windowValue.getFireTime(), windowValue.getPartitionNum() + "");
    }

    /**
     * create min session fire time, the current time+window size
     *
     * @param splitId
     * @param occurTime
     * @return
     */
    protected String createSessionFireTime(String splitId, Long occurTime) {
        Date newFireTime = createSessionFireDate(splitId, occurTime);
        return DateUtil.format(newFireTime);
    }


    /**
     * create min session fire time, the current time+window size
     *
     * @param splitId
     * @param lastUpdateTime
     * @return
     */
    protected Date createSessionFireDate(String splitId, Long lastUpdateTime) {
        if (lastUpdateTime == null) {
           // lastUpdateTime = this.updateMaxEventTime(splitId, (Long)null);
        }
        Date currentDate = new Date(lastUpdateTime);
        Date newFireTime = DateUtil.addSecond(currentDate, this.sizeInterval * this.timeUnitAdjust);
        return newFireTime;
    }

    @Override
    public long incrementAndGetSplitNumber(WindowInstance instance, String shuffleId) {
        long numer = super.incrementAndGetSplitNumber(instance, shuffleId);
        if (numer > 900000000) {
            this.getWindowMaxValueManager().resetSplitNum(instance, shuffleId);
        }
        return numer;
    }
}
