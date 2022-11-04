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
package org.apache.rocketmq.streams.openapi;

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

@AutoService(IChannelBuilder.class)
@ServiceName(value = OpenAPIChannelBuilder.TYPE, aliasName = "OpenAPISource")
public class OpenAPIChannelBuilder extends AbstractSupportShuffleChannelBuilder {
    public static final String TYPE = "openapi";

    @Override public ISource<?> createSource(String namespace, String name, Properties properties, MetaData metaData) {
        return (OpenAPISource) ConfigurableUtil.create(OpenAPISource.class.getName(), namespace, name, createFormatProperty(properties), null);
    }

    @Override public String getType() {
        return TYPE;
    }

    @Override public ISink<?> createSink(String namespace, String name, Properties properties, MetaData metaData) {
        throw new RuntimeException("can not support this method");
    }

    protected JSONObject createFormatProperty(Properties properties) {
        JSONObject formatProperties = new JSONObject();
        for (Object object : properties.keySet()) {
            String key = (String) object;
            if ("type".equals(key)) {
                continue;
            }
            formatProperties.put(key, properties.get(key));
        }
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "maxThread", "thread.max.count");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "groupName", "consumergroup");

        IChannelBuilder.formatPropertiesName(formatProperties, properties, "regionId", "region");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "accessKeyId", "ak");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "accessSecret", "sk");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "productName", "product");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "defaultParameterStr", "query");

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

    @Override public ISink<?> createBySource(ISource<?> pipelineSource) {
        return null;
    }
}
