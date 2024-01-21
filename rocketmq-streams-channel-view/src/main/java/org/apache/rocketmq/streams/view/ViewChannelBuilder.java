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

package org.apache.rocketmq.streams.view;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.builder.AbstractSupportShuffleChannelBuilder;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.builder.IShuffleChannelBuilder;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSink;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSource;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

@AutoService(IChannelBuilder.class)
@ServiceName(value = ViewChannelBuilder.TYPE, aliasName = "ViewSource")
public class ViewChannelBuilder extends AbstractSupportShuffleChannelBuilder {
    public static final String TYPE = "view";

    @Override public ISource createSource(String namespace, String name, Properties properties, MetaData metaData) {
        ViewSource viewSource = (ViewSource) ConfigurableUtil.create(ViewSource.class.getName(), namespace, name, createFormatProperty(properties), null);
        if (StringUtil.isEmpty(viewSource.getTableName())) {
            viewSource.setTableName(metaData.getTableName());
        }
        return viewSource;
    }

    @Override public String getType() {
        return TYPE;
    }

    @Override public ISink createSink(String namespace, String name, Properties properties, MetaData metaData) {
        ViewSink viewSink = (ViewSink) ConfigurableUtil.create(ViewSink.class.getName(), namespace, name, createFormatProperty(properties), null);
        if (StringUtil.isEmpty(viewSink.getViewTableName())) {
            viewSink.setViewTableName(metaData.getTableName());
        }
        return viewSink;
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
            formatProperties.put(key, properties.getProperty(key));
        }
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "viewTableName", "tableName");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "viewTableName", "viewName");
        IChannelBuilder.formatPropertiesName(formatProperties, properties, "viewTableName", "name");
//        IChannelBuilder.formatPropertiesName(formatProperties, properties, "maxThread", "thread.max.count");
        return formatProperties;
    }

    @Override
    public ISource copy(ISource pipelineSource) {
        ViewSource viewSource = (ViewSource) pipelineSource;
        if (viewSource.getRootSource() == null) {
            return null;
        }
        ServiceLoaderComponent serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        IChannelBuilder builder = (IChannelBuilder) serviceLoaderComponent.loadService(viewSource.getRootSource().getClass().getSimpleName());
        IShuffleChannelBuilder shuffleChannelBuilder = (IShuffleChannelBuilder) builder;
        return shuffleChannelBuilder.copy(((ViewSource) pipelineSource).getRootSource());
    }

    @Override
    public ISink createBySource(ISource pipelineSource) {
        ViewSource viewSource = (ViewSource) pipelineSource;
        if (viewSource.getRootSource() == null) {
            return null;
        }
        ServiceLoaderComponent serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        IChannelBuilder builder = (IChannelBuilder) serviceLoaderComponent.loadService(viewSource.getRootSource().getClass().getSimpleName());
        IShuffleChannelBuilder shuffleChannelBuilder = (IShuffleChannelBuilder) builder;
        return shuffleChannelBuilder.createBySource(viewSource.getRootSource());
    }
}
