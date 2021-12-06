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
package org.apache.rocketmq.streams.common.channel.impl.mutiltask;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;
@AutoService(IChannelBuilder.class)
@ServiceName(value = MutilSinkBuilder.SINK_TYPE, aliasName = "tasks",name="MutilTaskSink")
public class MutilSinkBuilder implements IChannelBuilder {
    public static final String SINK_TYPE="mutil_task";
    @Override public ISource createSource(String namespace, String name, Properties properties, MetaData metaData) {
        throw new RuntimeException("can not support this method");
    }



    @Override public ISink createSink(String namespace, String name, Properties properties, MetaData metaData) {
        MutilTaskSink sink = (MutilTaskSink) ConfigurableUtil.create(MutilTaskSink.class.getName(), namespace, name, createFormatProperty(properties), null);
        return sink;
    }



    protected JSONObject createFormatProperty(Properties properties) {
        JSONObject formatProperties = new JSONObject();
        for (Object object : properties.keySet()) {
            String key = (String) object;
            if ("type".equals(key)) {
                continue;
            }
            formatProperties.put(key, properties.getProperty(key));
        }
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "configureName", "taskName");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "configureName", "name");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "configureName", "task_name");
        return formatProperties;
    }


    @Override
    public String getType() {
       return MutilSinkBuilder.SINK_TYPE;
    }


}
