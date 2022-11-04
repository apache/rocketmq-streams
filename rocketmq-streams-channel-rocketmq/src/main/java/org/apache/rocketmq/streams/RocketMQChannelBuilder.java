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

package org.apache.rocketmq.streams;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.builder.AbstractSupportShuffleChannelBuilder;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;
import org.apache.rocketmq.streams.sink.RocketMQSink;
import org.apache.rocketmq.streams.source.RocketMQSource;

@AutoService(IChannelBuilder.class)
@ServiceName(value = RocketMQChannelBuilder.TYPE, aliasName = "RocketMQSource")
public class RocketMQChannelBuilder extends AbstractSupportShuffleChannelBuilder {
    public static final String TYPE = "rocketmq";

    @Override
    public ISource createSource(String namespace, String name, Properties properties, MetaData metaData) {
        return (RocketMQSource) ConfigurableUtil.create(RocketMQSource.class.getName(), namespace, name, createFormatProperty(properties), null);
    }

    @Override
    protected JSONObject createFormatProperty(Properties properties) {
        JSONObject formatProperties = new JSONObject();
        for (Object object : properties.keySet()) {
            String key = (String) object;
            if ("type".equals(key)) {
                continue;
            }
            formatProperties.put(key, properties.get(key));
        }
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "topic", "topic");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "tags", "tag");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "maxThread", "thread.max.count");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "pullIntervalMs", "pullIntervalMs");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "offsetTime", "offsetTime");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "namesrvAddr", "namesrvAddr");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "groupName", "producerGroup");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "groupName", "consumerGroup");

        IChannelBuilder.formatPropertiesName(formatProperties, properties, "maxThread", "maxthread");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "pullIntervalMs", "pullintervalms");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "offsetTime", "offsettime");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "namesrvAddr", "namesrvaddr");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "groupName", "producergroup");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "groupName", "consumergroup");
        if (properties.getProperty("group") != null) {
            String group = properties.getProperty("group");
            if (group.startsWith("GID_")) {
                formatProperties.put("groupName", group);
            } else {
                formatProperties.put("groupName", "GID_" + group);
            }
        }

        return formatProperties;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ISink createSink(String namespace, String name, Properties properties, MetaData metaData) {
        return (RocketMQSink) ConfigurableUtil.create(RocketMQSink.class.getName(), namespace, name, createFormatProperty(properties), null);
    }

    @Override
    public ISink createBySource(ISource pipelineSource) {
        RocketMQSource source = (RocketMQSource) pipelineSource;
        RocketMQSink sink = new RocketMQSink();
        sink.setNamesrvAddr(source.getNamesrvAddr());
        sink.setTopic(source.getTopic());
        sink.setTags(source.getTags());
        return sink;
    }
}
