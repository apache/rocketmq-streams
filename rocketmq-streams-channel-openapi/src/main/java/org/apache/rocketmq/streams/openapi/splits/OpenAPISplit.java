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
package org.apache.rocketmq.streams.openapi.splits;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

public class OpenAPISplit extends BasedConfigurable implements ISplit<OpenAPISplit, String> {
    protected String queueId;
    protected Long initStartTime;
    protected long dateAdd;
    protected Long pollingMinute;

    public OpenAPISplit(String queueId, Long initStartTime, long dateAdd, Long pollingMinute) {
        this.queueId = queueId;
        this.initStartTime = initStartTime;
        this.dateAdd = dateAdd;
        this.pollingMinute = pollingMinute;
    }

    public OpenAPISplit() {
    }

    @Override public String getQueueId() {
        return queueId;
    }

    @Override public String getQueue() {
        return queueId;
    }

    @Override public int compareTo(OpenAPISplit o) {
        return queueId.compareTo(o.queueId);
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public Long getInitStartTime() {
        return initStartTime;
    }

    public void setInitStartTime(Long initStartTime) {
        this.initStartTime = initStartTime;
    }

    public long getDateAdd() {
        return dateAdd;
    }

    public void setDateAdd(long dateAdd) {
        this.dateAdd = dateAdd;
    }

    public Long getPollingMinute() {
        return pollingMinute;
    }

    public void setPollingMinute(Long pollingMinute) {
        this.pollingMinute = pollingMinute;
    }
}
