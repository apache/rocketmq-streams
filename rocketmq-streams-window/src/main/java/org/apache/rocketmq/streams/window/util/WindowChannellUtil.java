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
package org.apache.rocketmq.streams.window.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemoryCache;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

public class WindowChannellUtil {
    public static String WINDOW_SHUFFLE_CHANNEL_TYPE = "window.shuffle.channel.type";//window 做shuffle中转需要的消息队列类型
    //比如rocketmq，需要topic，tags和group,属性值和字段名保持一致即可。配置如下:window.shuffle.channel.topic=abdc    window.shuffle.channel.tag=fdd

    public static String WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX = "window.shuffle.channel.";

    /**
     * create channel builder
     *
     * @return
     */
    protected static IChannelBuilder createBuilder(String connector) {
        String type = SystemContext.getProperty(connector);
        if (StringUtil.isEmpty(type)) {
            return null;
        }
        ServiceLoaderComponent serviceLoaderComponent = ComponentCreator.getComponent(
            IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        IChannelBuilder builder = (IChannelBuilder) serviceLoaderComponent.loadService(type);
        return builder;
    }

    /**
     * 创建channel，根据配置文件配置channel的连接信息
     *
     * @return
     */
    public static ISource createSource(String namespace, String name, String type, Properties sourceProperties,
        String keyPrefix, String dynamicPropertyValue) {

        IChannelBuilder builder = createBuilder(type);
        if (builder == null) {
            return null;
        }
        Properties properties = createChannelProperties(namespace, sourceProperties, keyPrefix, dynamicPropertyValue);
        ISource source = builder.createSource(namespace, name, properties, null);
        if (MemorySource.class.isInstance(source)) {
            MemorySource memorySource = (MemorySource) source;
            MemoryCache memoryCache = new MemoryCache();
            memorySource.setMemoryCache(memoryCache);
            memoryCache.init();
        }
        source.init();
        return source;
    }

    /**
     * 根据属性文件配置
     *
     * @return
     */
    protected static Properties createChannelProperties(String namespace, Properties sourceProperties, String keyPrefix,
        String dynamicPropertyValue) {
        Properties properties = new Properties();
        Iterator<Map.Entry<Object, Object>> it = sourceProperties.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Object, Object> entry = it.next();
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.startsWith(keyPrefix)) {
                String channelKey = key.replace(keyPrefix, "");
                if (channelKey.startsWith(namespace)) {//支持基于namespace 做shuffle window共享
                    channelKey = channelKey.replace(namespace, "");
                    properties.put(channelKey, value);
                } else {
                    if (!properties.containsKey(channelKey)) {
                        properties.put(channelKey, value);
                    }
                }

            }

        }
        Set<String> mutilPropertySet = new HashSet<>();
        String dynamicProperty = properties.getProperty("dynamic.property");
        if (dynamicProperty != null) {

            String[] mutilPropertys = dynamicProperty.split(",");

            for (String properyKey : mutilPropertys) {
                properties.put(properyKey, dynamicPropertyValue);
                mutilPropertySet.add(properyKey);
            }

        }
        putDynamicPropertyValue(mutilPropertySet, properties, dynamicPropertyValue);
        return properties;
    }

    protected static void putDynamicPropertyValue(Set<String> dynamiPropertySet, Properties properties,
        String dynamicPropertyValue) {
        String groupName = "groupName";
        if (!dynamiPropertySet.contains(groupName)) {
            properties.put(groupName, dynamicPropertyValue);
        }
        if (!dynamiPropertySet.contains("tags")) {
            properties.put("tags", dynamicPropertyValue);
        }
    }

}
