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

package org.apache.rocketmq.streams.client.source;

import com.google.common.collect.Sets;
import java.util.Set;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.source.RocketMQSource;

public class DataStreamSource {
    protected PipelineBuilder mainPipelineBuilder;
    protected Set<PipelineBuilder> otherPipelineBuilders;

    public DataStreamSource(String namespace, String pipelineName) {
        this.mainPipelineBuilder = new PipelineBuilder(namespace, pipelineName);
        this.otherPipelineBuilders = Sets.newHashSet();
    }

    public static DataStreamSource create(String namespace, String pipelineName) {
        return new DataStreamSource(namespace, pipelineName);
    }

    public DataStream fromFile(String filePath) {
        return fromFile(filePath, true);
    }

    public DataStream fromFile(String filePath, Boolean isJsonData) {
        FileSource fileChannel = new FileSource(filePath);
        fileChannel.setJsonData(isJsonData);
        this.mainPipelineBuilder.setSource(fileChannel);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, null);
    }

    public DataStream fromRocketmq(String topic, String groupName) {
        return fromRocketmq(topic, groupName, null, false);
    }

    public DataStream fromRocketmq(String topic, String groupName, boolean isJson) {
        return fromRocketmq(topic, groupName, null, isJson);
    }

    public DataStream fromRocketmq(String topic, String groupName, String tags, boolean isJson) {
        RocketMQSource rocketMQSource = new RocketMQSource();
        rocketMQSource.setTopic(topic);
        rocketMQSource.setTags(tags);
        rocketMQSource.setGroupName(groupName);
        rocketMQSource.setJsonData(isJson);
        this.mainPipelineBuilder.setSource(rocketMQSource);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, null);
    }

    public DataStream from(ISource<?> source) {
        this.mainPipelineBuilder.setSource(source);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, null);
    }

}
