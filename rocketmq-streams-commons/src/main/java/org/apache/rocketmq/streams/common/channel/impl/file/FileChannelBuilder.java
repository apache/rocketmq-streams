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
package org.apache.rocketmq.streams.common.channel.impl.file;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.builder.AbstractSupportShuffleChannelBuilder;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySink;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;

@AutoService(IChannelBuilder.class)
@ServiceName(value = FileChannelBuilder.TYPE, aliasName = "FileSource")
public class FileChannelBuilder extends AbstractSupportShuffleChannelBuilder {

    public static final String TYPE = "file";

    /**
     * @param namespace
     * @param name
     * @param properties
     * @return
     */
    @Override
    public ISource createSource(String namespace, String name, Properties properties, MetaData metaData) {

        FileSource fileSource = (FileSource) ConfigurableUtil.create(FileSource.class.getName(), namespace, name, createFormatProperty(properties), null);
        return fileSource;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ISink createSink(String namespace, String name, Properties properties, MetaData metaData) {
        FileSink fileSink = (FileSink) ConfigurableUtil.create(FileSink.class.getName(), namespace, name, createFormatProperty(properties), null);
        return fileSink;
    }

    /**
     * 创建标准的属性文件
     *
     * @param properties
     * @return
     */
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
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "filePath", "filePath");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "filePath", "filepath");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "filePath", "fileName");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "maxThread", "thread.max.count");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "maxThread", "maxthread");
        return formatProperties;
    }

    @Override
    public ISource copy(ISource pipelineSource) {
        return new MemorySource();
    }

    @Override
    public ISink createBySource(ISource pipelineSource) {
        return new MemorySink();
    }
}
