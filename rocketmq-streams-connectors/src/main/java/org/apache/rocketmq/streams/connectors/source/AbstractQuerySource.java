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
package org.apache.rocketmq.streams.connectors.source;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.connectors.model.QuerySplit;

public abstract class AbstractQuerySource extends AbstractPullSource {
    protected String initFirstTime = DateUtil.getCurrentTimeString();
    protected Long pollingMinute = 10L;
    protected int splitCount = 1;//在一个周期内的并发性
    protected int pageInit=0;


    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        List<ISplit<?, ?>> splits = new ArrayList<>();
        long baseDataAdd = pollingMinute / splitCount;
        long remainder = pollingMinute % splitCount;
        Date date = DateUtil.getWindowBeginTime(DateUtil.parseTime(initFirstTime).getTime(), pollingMinute*60*1000);
        for (int i = 0; i < splitCount; i++) {
            long dataAdd = baseDataAdd + (remainder > 0 ? 1 : 0);
            remainder--;
            Date endDate = DateUtil.addMinute(date, (int) dataAdd);
            QuerySplit openAPISplit = new QuerySplit(String.valueOf(i), date.getTime(), dataAdd, pollingMinute,pageInit);
            splits.add(openAPISplit);
            date = endDate;
        }
        return splits;
    }

    protected long getEndTime(long startTime,long dateAdd){
        return startTime + dateAdd * 60 * 1000;
    }

    public String getInitFirstTime() {
        return initFirstTime;
    }

    public void setInitFirstTime(String initFirstTime) {
        this.initFirstTime = initFirstTime;
    }

    public Long getPollingMinute() {
        return pollingMinute;
    }

    public void setPollingMinute(Long pollingMinute) {
        this.pollingMinute = pollingMinute;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public void setSplitCount(int splitCount) {
        this.splitCount = splitCount;
    }

    public int getPageInit() {
        return pageInit;
    }

    public void setPageInit(int pageInit) {
        this.pageInit = pageInit;
    }
}
