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

import com.alibaba.fastjson.JSONObject;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.source.SplitProgress;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;

public class SourceMetric {
    /**
     * metric info
     */
    protected AtomicLong outCount = new AtomicLong(0);
    protected Long firstReceiveTime;
    protected Long lastReceiveTime;
    protected Long startTime;
    protected volatile long avgCostTime = 0;
    protected volatile long maxCostTime = 0;
    protected AtomicLong sumCostTime = new AtomicLong(0);
    protected Map<String, SplitProgress> splitProgresses;
    protected ISource source;
    protected MetaData metaData;

    public long endCalculate(long startTime) {
        long cost = System.currentTimeMillis() - startTime;
        long sum = this.sumCostTime.addAndGet(cost);
        long count = outCount.incrementAndGet();
        this.avgCostTime = sum / count;
        if (this.maxCostTime < cost) {
            this.maxCostTime = cost;
        }
        return cost;
    }

    public long incrementOut() {
        return outCount.incrementAndGet();
    }

    public Long getOutCount() {
        return outCount.get();
    }

    public void setOutCount(AtomicLong outCount) {
        this.outCount = outCount;
    }

    public String lastMsgReceive() {
        if (lastReceiveTime == null) {
            return null;
        }
        return DateUtil.longToString(lastReceiveTime);
    }

    public String firstMsgReceive() {
        if (firstReceiveTime == null) {
            return null;
        }
        return DateUtil.longToString(firstReceiveTime);
    }

    public String startTime() {
        if (startTime == null) {
            return null;
        }
        return DateUtil.longToString(startTime);
    }

    public String toProgress() {
        if (splitProgresses == null) {
            return "还未采集到，请检查SQL Source with部分有没有配置sendLogTimeFieldName字段，这个字段是产生消息的时间，格式是时间戳或2022-09-01 12:12:12";
        }
        JSONObject jsonObject = new JSONObject();
        for (SplitProgress progress : this.splitProgresses.values()) {
            jsonObject.put("分片（" + progress.getSplitId() + "）延迟" + (progress.isTimeStamp() ? "时间（毫秒）" : "条数"), progress.getProgress() < 0 ? 0 : progress.getProgress() + "---(更新时间：" + DateUtil.getCurrentTimeString() + ")");
        }
        return JsonableUtil.formatJson(jsonObject);
    }

    public String toCost() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("平均消耗时间", avgCostTime);
        jsonObject.put("最大消耗时间", maxCostTime);
        jsonObject.put("总消耗时间", sumCostTime);
        return JsonableUtil.formatJson(jsonObject);
    }

    public void setMsgReceivTime(Long time) {
        if (firstReceiveTime == null) {
            firstReceiveTime = time;
        }
        lastReceiveTime = time;
    }

    public Long getFirstReceiveTime() {
        return firstReceiveTime;
    }

    public void setFirstReceiveTime(Long firstReceiveTime) {
        this.firstReceiveTime = firstReceiveTime;
    }

    public Long getLastReceiveTime() {
        return lastReceiveTime;
    }

    public void setLastReceiveTime(Long lastReceiveTime) {
        this.lastReceiveTime = lastReceiveTime;
    }

    public Map<String, SplitProgress> getSplitProgresses() {
        return splitProgresses;
    }

    public void setSplitProgresses(
        Map<String, SplitProgress> splitProgresses) {
        this.splitProgresses = splitProgresses;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public long getAvgCostTime() {
        return avgCostTime;
    }

    public void setAvgCostTime(long avgCostTime) {
        this.avgCostTime = avgCostTime;
    }

    public long getMaxCostTime() {
        return maxCostTime;
    }

    public void setMaxCostTime(long maxCostTime) {
        this.maxCostTime = maxCostTime;
    }

    public long getSumCostTime() {
        return sumCostTime.get();
    }

    public ISource getSource() {
        return source;
    }

    public void setSource(ISource source) {
        this.source = source;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }
}
