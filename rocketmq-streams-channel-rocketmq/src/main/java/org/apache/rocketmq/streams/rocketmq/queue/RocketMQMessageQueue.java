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
package org.apache.rocketmq.streams.rocketmq.queue;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class RocketMQMessageQueue extends BasedConfigurable implements ISplit<RocketMQMessageQueue, MessageQueue> {
    protected transient MessageQueue queue;
    protected String brokeName;
    protected String topic;
    protected int mqQueueId;

    public RocketMQMessageQueue(MessageQueue queue) {
        this.queue = queue;
        this.brokeName = queue.getBrokerName();
        this.topic = queue.getTopic();
        this.mqQueueId = queue.getQueueId();
    }

    public RocketMQMessageQueue() {

    }

    public static String getQueueId(MessageQueue queue) {

        String[] topic = queue.getTopic().split("%");
        if (topic.length > 1) {
            return MapKeyUtil.createKeyBySign("_", topic[1], queue.getBrokerName(), getSplitNumerStr(queue.getQueueId()) + "");
        }
        return MapKeyUtil.createKeyBySign("_", queue.getTopic(), queue.getBrokerName(), getSplitNumerStr(queue.getQueueId()) + "");
    }

    /**
     * 获取分片的字符串格式，需要3位对齐
     *
     * @param splitNumer
     * @return
     */
    private static String getSplitNumerStr(int splitNumer) {
        int len = (splitNumer + "").length();
        if (len == 3) {
            return splitNumer + "";
        }
        String splitNumerStr = splitNumer + "";
        while (len < 3) {
            splitNumerStr = "0" + splitNumerStr;
            len = splitNumerStr.length();
        }
        return splitNumerStr;
    }

    @Override
    protected void getJsonObject(JSONObject jsonObject) {
        super.getJsonObject(jsonObject);
        queue = new MessageQueue(topic, brokeName, mqQueueId);
    }

    @Override
    public MessageQueue getQueue() {
        return queue;
    }

    @Override
    public int compareTo(RocketMQMessageQueue o) {
        return queue.compareTo(o.queue);
    }

    @Override
    public String getQueueId() {
        return getQueueId(this.queue);
    }

    public String getBrokeName() {
        return brokeName;
    }

    public void setBrokeName(String brokeName) {
        this.brokeName = brokeName;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public int getMqQueueId() {
        return mqQueueId;
    }

    public void setMqQueueId(int mqQueueId) {
        this.mqQueueId = mqQueueId;
    }

}
