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
package org.apache.rocketmq.streams.common.topology.metric;

import com.alibaba.fastjson.JSONArray;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;

public class StageMetric {
    /**
     * metric info
     */
    protected AtomicLong inCount=new AtomicLong(0);
    protected AtomicLong outCount=new AtomicLong(0);
    protected transient Long firstReceiveTime;
    protected double qps;

    protected long maxCostTime;
    protected long avgCostTime;
    protected transient long sumCostTime;

    protected List<NotFireReason> notFireReasons=new ArrayList<>();

    public long startCalculate(IMessage msg){
        if(firstReceiveTime==null){
            firstReceiveTime=System.currentTimeMillis();
        }
        long countValue=inCount.incrementAndGet();
        long timeGap=(System.currentTimeMillis()-firstReceiveTime)/1000;
        if(timeGap<1) {
            return System.currentTimeMillis();
        }
        qps=countValue/timeGap;
        return System.currentTimeMillis();
    }

    public void endCalculate(long startTime){
        long cost=System.currentTimeMillis()-startTime;
        this.sumCostTime+=cost;
        this.avgCostTime=this.sumCostTime/inCount.get();
        if(this.maxCostTime<cost){
            this.maxCostTime=cost;
        }

    }
    public void outCalculate() {
        outCount.incrementAndGet();
    }

    public void filterCalculate(NotFireReason notFireReason){
        if(!notFireReasons.contains(notFireReason)){
            this.notFireReasons.add(notFireReason);
        }
        while (notFireReasons.size()>100){
            notFireReasons.remove(0);
        }


    }

    public Long getFirstReceiveTime() {
        return firstReceiveTime;
    }

    public void setFirstReceiveTime(Long firstReceiveTime) {
        this.firstReceiveTime = firstReceiveTime;
    }

    public double getQps() {
        return qps;
    }

    public void setQps(double qps) {
        this.qps = qps;
    }

    public long getMaxCostTime() {
        return maxCostTime;
    }

    public void setMaxCostTime(long maxCostTime) {
        this.maxCostTime = maxCostTime;
    }

    public long getAvgCostTime() {
        return avgCostTime;
    }

    public void setAvgCostTime(long avgCostTime) {
        this.avgCostTime = avgCostTime;
    }

    public long getSumCostTime() {
        return sumCostTime;
    }

    public void setSumCostTime(long sumCostTime) {
        this.sumCostTime = sumCostTime;
    }

    public Long getInCount() {
        return inCount.get();
    }
    public Long getOutCount() {
        return outCount.get();
    }

    public List<NotFireReason> getNotFireReasons() {
        return notFireReasons;
    }

    public String createNotFireReason() {
        JSONArray jsonArray=new JSONArray();
        for(NotFireReason notFireReason:this.notFireReasons){
            jsonArray.add(notFireReason.toJson());
        }
        return JsonableUtil.formatJson(jsonArray);
    }


}
