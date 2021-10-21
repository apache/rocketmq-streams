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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;

public class ShuffleOverWindow extends WindowOperator {
    protected static String TOPN_KEY="___TopN_";
    protected transient List<OrderBy> orderList;
    protected List<String> orderFieldNames;//name contains 2 part:name;true/false
    protected int topN=100;
    protected transient Long firstUpdateTime;
    /**
     * 需要把生成的序列号返回设置到message，这个是序列号对应的名字
     */
    protected String rowNumerName;

    @Override protected boolean initConfigurable() {
         boolean success= super.initConfigurable();
         this.setFireMode(2);
         this.setSizeInterval(5);
         this.setSlideInterval(5);
         this.setTimeUnitAdjust(1);
         this.setWaterMarkMinute(3600);
         return success;
    }


    @Override protected void calculateWindowValue(WindowValue windowValue, IMessage msg) {
        super.calculateWindowValue(windowValue, msg);
        TopNState topNState=(TopNState)windowValue.getAggColumnResultByKey(TOPN_KEY);
        if(topNState==null){
            topNState=new TopNState(topN);

        }
        topNState.addAndSortMsg(msg.getMessageBody(),orderList);
        windowValue.putAggColumnResult(TOPN_KEY,topNState);
    }

    @Override public void sendFireMessage(List<WindowValue> windowValueList, String queueId) {
        List<WindowValue> windowValues=new ArrayList<>();
        for(WindowValue windowValue:windowValueList){

            TopNState topNState=(TopNState)windowValue.getAggColumnResultByKey(TOPN_KEY);


            if(topNState.isChanged()){
                int i=0;
                for(Map<String,Object> msg:topNState.getOrderMsgs(this.rowNumerName,this.getSelectMap().keySet())){
                    WindowValue copy=windowValue.clone();
                    copy.setAggColumnMap(new HashMap<>());
                    copy.setPartitionNum(copy.getPartitionNum()*topN);
                    copy.setPartitionNum(copy.getPartitionNum()+i);
                    copy.putComputedColumnResult(msg);
                    windowValues.add(copy);
                    i++;
                }
                topNState.setChanged(false);
            }

        }
        super.sendFireMessage(windowValues, queueId);
    }



    /**
     * 根据消息获取对应的window instance 列表
     *
     * @param message
     * @return
     */
    @Override
    public List<WindowInstance> queryOrCreateWindowInstance(IMessage message,String queueId) {
        return  WindowInstance.getOrCreateWindowInstance(this, getOccurTime(this, message), timeUnitAdjust,
            queueId);
    }

    @Override
    public void logoutWindowInstance(String indexId) {
        super.logoutWindowInstance(indexId);
        this.firstUpdateTime=null;
    }
    public  Long getOccurTime(AbstractWindow window, IMessage message) {
        if(this.firstUpdateTime==null){
            this.firstUpdateTime=System.currentTimeMillis();
        }
        return this.firstUpdateTime;
    }

    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        super.doProcessAfterRefreshConfigurable(configurableService);
        if(orderList==null){
            synchronized (this){
                if(orderList==null){
                    List<OrderBy> list=new ArrayList<>();
                    for(String name:orderFieldNames){
                        String[] values=name.split(";");
                        String fieldName=values[0];
                        Boolean isAsc=Boolean.valueOf(values[1]);
                        OrderBy orderBy=new OrderBy(fieldName,isAsc);
                        list.add(orderBy);
                    }
                    this.orderList=list;
                }
            }
        }
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
}
