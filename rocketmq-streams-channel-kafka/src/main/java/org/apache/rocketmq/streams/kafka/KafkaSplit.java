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
package org.apache.rocketmq.streams.kafka;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.common.PartitionInfo;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

public class KafkaSplit extends BasedConfigurable implements ISplit<KafkaSplit, PartitionInfo> {
    protected PartitionInfo partitionInfo;
    protected String topic;
    protected int partition;

    public KafkaSplit(PartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
        this.partition = partitionInfo.partition();
    }

    @Override
    public String getQueueId() {
        return partition + "";
    }

    @Override
    public PartitionInfo getQueue() {
        return partitionInfo;
    }

    @Override
    public int compareTo(KafkaSplit o) {
        return partition - o.partition;
    }

    @Override
    protected void getJsonObject(JSONObject jsonObject) {
        super.getJsonObject(jsonObject);
        this.partitionInfo = new PartitionInfo(topic, partition, null, null, null);
    }
}
