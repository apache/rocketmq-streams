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
package org.apache.rocketmq.streams.huawei.obs.builder;

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
import org.apache.rocketmq.streams.huawei.obs.ObsSource;

@AutoService(IChannelBuilder.class)
@ServiceName(value = ObsSourceBuilder.TYPE, aliasName = "ObsSource")
public class ObsSourceBuilder extends AbstractSupportShuffleChannelBuilder {

    public static final String TYPE = "huawei_obs";

    @Override
    public ISource<?> createSource(String namespace, String name, Properties properties, MetaData metaData) {
        return (ObsSource)ConfigurableUtil.create(ObsSource.class.getName(), namespace, name, createFormatProperty(properties), null);
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ISink<?> createSink(String namespace, String name, Properties properties, MetaData metaData) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public ISink<?> createBySource(ISource<?> pipelineSource) {
        return null;
    }

    @Override
    protected JSONObject createFormatProperty(Properties properties) {
        JSONObject formatProperties = new JSONObject();
        for (Object object : properties.keySet()) {
            String key = (String)object;
            if ("type".equals(key)) {
                continue;
            }
            formatProperties.put(key, properties.get(key));
        }

        IChannelBuilder.formatPropertiesName(formatProperties, properties, "accessId", "ak");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "accessKey", "sk");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "endPoint", "end_point");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "bucketName", "bucket_name");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "filePath", "file_path");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "filePrefix", "file_prefix");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "fileType", "file_type");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "fieldDelimiter", "field_delimiter");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "compressType", "compress_type");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "cycle", "cycle");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "cycleUnit", "cycle_unit");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "cycleT", "cycle_t");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "startTime", "start_time");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "endTime", "end_time");

        return formatProperties;
    }

}
