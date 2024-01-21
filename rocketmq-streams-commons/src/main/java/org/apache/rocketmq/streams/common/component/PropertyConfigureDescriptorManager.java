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
package org.apache.rocketmq.streams.common.component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.streams.common.utils.ENVUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertyConfigureDescriptorManager {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyConfigureDescriptorManager.class);

    protected transient Map<String, List<ConfigureDescriptor>> groupByConfigures = new HashMap<>();

    /**
     * 多组配置文件，比如http是一组，db是一组。需要子类在启动时在数据中写如配置描述
     */
    protected transient List<ConfigureDescriptor> defaultGroupConfigureDescriptors = new ArrayList<>();

    /**
     * 增加一个属性配置
     *
     * @param configureDescriptor
     */
    public void addConfigureDescriptor(ConfigureDescriptor configureDescriptor) {
        if (configureDescriptor == null) {
            return;
        }
        String groupName = configureDescriptor.getGroupName();
        if (StringUtil.isEmpty(groupName)) {
            defaultGroupConfigureDescriptors.add(configureDescriptor);
            return;
        }
        List<ConfigureDescriptor> configureDescriptors = groupByConfigures.get(groupName);
        if (configureDescriptors == null) {
            synchronized (this) {
                configureDescriptors = groupByConfigures.computeIfAbsent(groupName, k -> new ArrayList<>());
            }
        }
        configureDescriptors.add(configureDescriptor);
    }

    /**
     * 对于一组配置进行检查，如果所有的必须参数都在环境变量中，则创建属性文件
     *
     * @param configureDescriptorList
     * @return
     */
    public Properties createENVProperties(List<ConfigureDescriptor> configureDescriptorList) {
        if (configureDescriptorList == null || configureDescriptorList.size() == 0) {
            return null;
        }
        Properties properties = new Properties();
        for (ConfigureDescriptor configureDescriptor : configureDescriptorList) {
            String key = configureDescriptor.getEnvPropertyKey();
            String value = ENVUtil.getENVParameter(key);
            if (configureDescriptor.isRequiredSet() && value == null) {
                return null;
            }
            if (value != null) {
                properties.put(configureDescriptor.getPropertyKey(), value);
            } else {
                value = configureDescriptor.getDefaultValue();
                if (StringUtil.isNotEmpty(value)) {
                    properties.put(configureDescriptor.getPropertyKey(), configureDescriptor.getDefaultValue());
                }
            }
        }
        LOG.info("env properties:" + properties.entrySet());
        return properties;
    }

    public Map<String, List<ConfigureDescriptor>> getGroupByConfigures() {
        Map<String, List<ConfigureDescriptor>> map = new HashMap<>();
        Iterator<Map.Entry<String, List<ConfigureDescriptor>>> it = groupByConfigures.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, List<ConfigureDescriptor>> entry = it.next();
            String groupName = entry.getKey();
            List<ConfigureDescriptor> value = entry.getValue();
            List<ConfigureDescriptor> newValue = new ArrayList<>();
            newValue.addAll(value);
            newValue.addAll(defaultGroupConfigureDescriptors);
            map.put(groupName, newValue);
        }
        return map;
    }
}
