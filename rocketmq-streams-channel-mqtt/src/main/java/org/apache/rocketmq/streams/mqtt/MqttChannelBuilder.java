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
package org.apache.rocketmq.streams.mqtt;

import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.builder.AbstractSupportShuffleChannelBuilder;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;
import org.apache.rocketmq.streams.mqtt.sink.PahoSink;
import org.apache.rocketmq.streams.mqtt.source.PahoSource;

@AutoService(IChannelBuilder.class)
@ServiceName(value = MqttChannelBuilder.TYPE, aliasName = "PahoSource")
public class MqttChannelBuilder extends AbstractSupportShuffleChannelBuilder {
    public static final String TYPE = "mqtt";

    @Override
    public ISource createSource(String namespace, String name, Properties properties, MetaData metaData) {
        return (ISource<?>) ConfigurableUtil.create(PahoSource.class.getName(), namespace, name, createFormatProperty(properties), null);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ISink createSink(String namespace, String name, Properties properties, MetaData metaData) {
        return (ISink<?>) ConfigurableUtil.create(PahoSink.class.getName(), namespace, name, createFormatProperty(properties), null);
    }

    @Override
    public ISink createBySource(ISource pipelineSource) {
        return new MemorySink();
    }
}
