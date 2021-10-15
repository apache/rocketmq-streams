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

import java.util.Properties;
import javax.sql.DataSource;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.source.RocketMQSource;

import java.io.Serializable;

public class DataStreamSource implements Serializable {
    protected PipelineBuilder mainPipelineBuilder;

    public DataStreamSource(String namespace, String pipelineName) {
        this.mainPipelineBuilder = new PipelineBuilder(namespace, pipelineName);
    }

    public DataStreamSource(String namespace, String pipelineName, String[] duplicateKeys, Long windowSize) {
        this.mainPipelineBuilder = new PipelineBuilder(namespace, pipelineName);
        Properties properties = new Properties();
        properties.setProperty(pipelineName + ".duplicate.fields.names", String.join(";", duplicateKeys));
        properties.setProperty(pipelineName + ".duplicate.expiration.time", String.valueOf(windowSize));
        ComponentCreator.createProperties(properties);
    }

    public static DataStreamSource create(String namespace, String pipelineName) {
        return new DataStreamSource(namespace, pipelineName);
    }

    public static DataStreamSource create(String namespace, String pipelineName, String[] duplicateKeys,
        Long windowSize) {
        return new DataStreamSource(namespace, pipelineName, duplicateKeys, windowSize);
    }

    public DataStream fromFile(String filePath) {
        return fromFile(filePath, true);
    }

    public DataStream fromFile(String filePath, Boolean isJsonData) {
        FileSource fileChannel = new FileSource(filePath);
        fileChannel.setJsonData(isJsonData);
        this.mainPipelineBuilder.setSource(fileChannel);
        return new DataStream(this.mainPipelineBuilder, null);
    }

    public DataStream fromRocketmq(String topic, String groupName, String namesrvAddress) {
        return fromRocketmq(topic, groupName, false, namesrvAddress);
    }

    public DataStream fromRocketmq(String topic, String groupName, boolean isJson, String namesrvAddress) {
        return fromRocketmq(topic, groupName, "*", isJson, namesrvAddress);
    }

    public DataStream fromRocketmq(String topic, String groupName, String tags, boolean isJson, String namesrvAddress) {
        RocketMQSource rocketMQSource = new RocketMQSource();
        rocketMQSource.setTopic(topic);
        rocketMQSource.setTags(tags);
        rocketMQSource.setGroupName(groupName);
        rocketMQSource.setJsonData(isJson);
        rocketMQSource.setNamesrvAddr(namesrvAddress);
        this.mainPipelineBuilder.setSource(rocketMQSource);
        return new DataStream(this.mainPipelineBuilder, null);
    }

    public DataStream from(ISource<?> source) {
        this.mainPipelineBuilder.setSource(source);
        return new DataStream(this.mainPipelineBuilder, null);
    }

}
