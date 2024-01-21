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

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.utils.ENVUtil;
import org.apache.rocketmq.streams.common.utils.PropertiesUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractComponent<T> implements IComponent<T> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractComponent.class);
    /**
     * xml的位置，如果没有即默认位置
     */
    protected PropertyConfigureDescriptorManager configureDescriptorManager = new PropertyConfigureDescriptorManager();
    protected AtomicBoolean isStart = new AtomicBoolean(false);
    private Properties properties;

    @Override public boolean init() {
        Properties properties = createDefaultProperty();
        return initProperty(properties);
    }

    protected Properties createDefaultProperty() {
        //createENVProperties();
        Properties properties = getDefaultProperties();
        if (properties == null) {
            properties = new Properties();
        }
        Properties newProperties = new Properties();
        newProperties.putAll(properties);
        addSystemProperties(newProperties);
        return newProperties;
    }

    public void initConfigurableServiceDescriptor() {
        addConfigureDescriptor(new ConfigureDescriptor("jdbc", ConfigurationKey.JDBC_URL, null, true, ConfigurationKey.ENV_JDBC_URL));
        addConfigureDescriptor(new ConfigureDescriptor("jdbc", ConfigurationKey.JDBC_USERNAME, null, true, ConfigurationKey.ENV_JDBC_USERNAME));
        addConfigureDescriptor(new ConfigureDescriptor("jdbc", ConfigurationKey.JDBC_PASSWORD, null, true, ConfigurationKey.ENV_JDBC_PASSWORD));
        addConfigureDescriptor(new ConfigureDescriptor("jdbc", ConfigurationKey.JDBC_DRIVER, ConfigurationKey.DEFAULT_JDBC_DRIVER, false, ConfigurationKey.ENV_JDBC_DRIVER));
        addConfigureDescriptor(new ConfigureDescriptor("http", ConfigurationKey.HTTP_AK, true));
        addConfigureDescriptor(new ConfigureDescriptor("http", ConfigurationKey.HTTP_SK, true));
    }

    protected void addConfigureDescriptor(ConfigureDescriptor configureDescriptor) {
        configureDescriptorManager.addConfigureDescriptor(configureDescriptor);
    }

    /**
     * 如果系统属性中有对应的属性配置，覆盖掉环境变量和文件的属性，主要用于测试，不推荐用在生产环境
     *
     * @param properties
     */
    protected void addSystemProperties(Properties properties) {
        for (List<ConfigureDescriptor> configureDescriptors : configureDescriptorManager.getGroupByConfigures().values()) {
            for (ConfigureDescriptor configureDescriptor : configureDescriptors) {
                String key = configureDescriptor.getPropertyKey();
                String value = ENVUtil.getSystemParameter(key);
                if (value != null) {
                    properties.put(key, value);
                }
            }
        }
    }

    /**
     * 根据配置文件的配置情况，进行环境变量检查，如果所有的必须参数都在环境变量中，则创建属性文件。支持多组属性，如configure，既支持http配置，也支持db配置
     *
     * @return
     */
    protected Properties createENVProperties() {
        if (configureDescriptorManager.getGroupByConfigures() == null) {
            return null;
        }
        Iterator<List<ConfigureDescriptor>> it = configureDescriptorManager.getGroupByConfigures().values().iterator();
        Properties properties = new Properties();
        boolean hasProperties = false;
        while (it.hasNext()) {
            List<ConfigureDescriptor> configureDiscriptors = it.next();
            Properties p = configureDescriptorManager.createENVProperties(configureDiscriptors);
            if (p != null) {
                properties.putAll(p);
                hasProperties = true;
            }
        }
        if (hasProperties) {
            return properties;
        }
        return null;
    }

    @Override public boolean initByClassPath(String propertiesPath) {
        Properties properties = PropertiesUtil.getResourceProperties(propertiesPath);
        return initProperty(properties);
    }

    @Override public boolean initByFilePath(String filePath) {
        Properties properties = PropertiesUtil.loadPropertyByFilePath(filePath);
        return initProperty(properties);
    }

    @Override public boolean initByPropertiesStr(String... kvs) {
        Properties properties = createDefaultProperty();
        if (kvs != null && kvs.length > 0) {
            for (String ky : kvs) {
                PropertiesUtil.putProperty(ky, ":", properties);
            }
        }
        return initProperty(properties);
    }

    /**
     * 根据属性文件init component
     *
     * @param properties
     * @return
     */
    protected boolean initProperty(Properties properties) {
        boolean success = initProperties(properties);
        this.properties = properties;
        return success;
    }

    protected Properties getDefaultProperties() {
        try {
            return PropertiesUtil.getResourceProperties(getComponentPropertyPath());
        } catch (Exception e) {
            LOG.error("load jar file error", e);
            return null;
        }

    }

    private String getComponentPropertyPath() {
        return PropertiesUtil.getComponentPropertyPath(this.getClass());
    }

    @Override public boolean start(String name) {
        if (isStart.compareAndSet(false, true)) {
            startComponent(name);
        }
        return true;
    }

    protected abstract boolean startComponent(String name);

    protected void finishStart() {
        isStart.set(true);
    }

    public Properties getProperties() {
        return properties;
    }

    protected abstract boolean initProperties(Properties properties);

    public boolean directInitProperties(Properties properties) {
        return initProperties(properties);
    }
}
