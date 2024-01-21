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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;

public class ShuffleOverWindow extends WindowOperator {
    protected static String TOPN_KEY = "___TopN_";
    protected transient List<OrderBy> orderList;
    protected List<String> orderFieldNames;//name contains 2 part:name;true/false
    protected int topN = 100;
    /**
     * 需要把生成的序列号返回设置到message，这个是序列号对应的名字
     */
    protected String rowNumerName = "rowNum";

    protected int startRowNum;//默认为0，当设置这个值时，小于这个值的行被丢弃

    protected transient Long lastFireTime;
    protected transient Long lastChangeTime;

    @Override protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        if (this.getSizeInterval() == 0) {
            String windowSizeStr = getConfiguration().getProperty(ConfigurationKey.DIPPER_WINDOW_OVER_DEFAULT_INTERVAL_SIZE);
            int windowSize = 3600;
            if (StringUtil.isNotEmpty(windowSizeStr)) {
                windowSize = Integer.valueOf(windowSizeStr);
            }
            this.setSizeInterval(windowSize);
            this.setSlideInterval(windowSize);
        }
        if (this.getEmitBeforeValue() == null) {
            String emitBeforeStr = getConfiguration().getProperty(ConfigurationKey.DIPPER_WINDOW_OVER_DEFAULT_EMIT_BEFORE_SECOND);
            long emitBefore = 60L;
            if (StringUtil.isNotEmpty(emitBeforeStr)) {
                emitBefore = Integer.valueOf(emitBeforeStr);
            }

            this.setEmitBeforeValue(emitBefore);
        }
        this.setTimeUnitAdjust(1);

        if (this.orderFieldNames == null) {
            return true;
        }
        if (orderList == null) {
            synchronized (this) {
                if (orderList == null) {
                    List<OrderBy> list = new ArrayList<>();
                    for (String name : orderFieldNames) {
                        String[] values = name.split(";");
                        String fieldName = values[0];
                        Boolean isAsc = Boolean.valueOf(values[1]);
                        OrderBy orderBy = new OrderBy(fieldName, isAsc);
                        list.add(orderBy);
                    }
                    this.orderList = list;
                }
            }
        }
        return success;
    }

    @Override protected void calculateWindowValue(WindowValue windowValue, IMessage msg) {
        super.calculateWindowValue(windowValue, msg);
        TopNState topNState = (TopNState) windowValue.getAggColumnResultByKey(TOPN_KEY);
        if (topNState == null) {
            topNState = new TopNState(topN);

        }
        boolean isChanged = topNState.addAndSortMsg(msg.getMessageBody(), orderList);
        if (isChanged) {
            lastChangeTime = System.currentTimeMillis();
        }
        windowValue.putAggColumnResult(TOPN_KEY, topNState);
    }

    @Override
    public int fireWindowInstance(WindowInstance instance, String queueId) {
        if (lastFireTime != null && lastChangeTime != null) {
            if (lastFireTime - lastChangeTime > 0) {
                return 0;
            }
        }
        int result = super.fireWindowInstance(instance, queueId);
        lastFireTime = System.currentTimeMillis();
        return result;
    }

    @Override public void sendFireMessage(List<WindowValue> windowValueList, String queueId) {
        List<WindowValue> windowValues = new ArrayList<>();
        for (WindowValue windowValue : windowValueList) {

            TopNState topNState = (TopNState) windowValue.getAggColumnResultByKey(TOPN_KEY);

            int i = 0;
            for (Map<String, Object> msg : topNState.getOrderMsgs(this.rowNumerName, this.getSelectMap().keySet(), this.startRowNum)) {
                WindowValue copy = windowValue.clone();
                copy.setAggColumnMap(new HashMap<>());
                copy.setPartitionNum(copy.getPartitionNum() * topN + i);
                copy.putComputedColumnResult(msg);
                windowValues.add(copy);
                i++;
            }

        }
        super.sendFireMessage(windowValues, queueId);
    }

    public List<String> getOrderFieldNames() {
        return orderFieldNames;
    }

    public void setOrderFieldNames(List<String> orderFieldNames) {
        this.orderFieldNames = orderFieldNames;
    }

    public int getTopN() {
        return topN;
    }

    public void setTopN(int topN) {
        this.topN = topN;
    }

    public String getRowNumerName() {
        return rowNumerName;
    }

    public void setRowNumerName(String rowNumerName) {
        this.rowNumerName = rowNumerName;
    }

    public int getStartRowNum() {
        return startRowNum;
    }

    public void setStartRowNum(int startRowNum) {
        this.startRowNum = startRowNum;
    }
}
