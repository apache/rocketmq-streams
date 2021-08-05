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

package org.apache.rocketmq.streams.configuable;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.component.ConfigureDescriptor;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;
import org.apache.rocketmq.streams.configuable.service.AbstractConfigurableService;
import org.apache.rocketmq.streams.configuable.service.ConfigurableServcieType;
import org.apache.rocketmq.streams.configuable.service.ConfigurableServiceFactory;

/**
 * 对Configurable对象，做统一的管理，统一查询，插入和更新。 insert/update 把configuabel对象写入存储，支持文件存储（file），内存存储（memory）和db存储（DB）。可以在配置通过这个ConfigureFileKey.CONNECT_TYPE key 配置 query 是基于内存的查询，对象定时load到内存，可以在属性文件通过这个ConfigureFileKey.POLLING_TIME key配置加载周期，单位是秒 新对象加载后生效，已经存在的对象只有updateFlag发生变化才会被替换
 */
public class ConfigurableComponent extends AbstractComponent<IConfigurableService>
    implements IConfigurableService {

    private static final Log LOG = LogFactory.getLog(ConfigurableComponent.class);

    protected volatile IConfigurableService configureService = null;

    protected transient String namespace;

    public ConfigurableComponent() {
        initConfigurableServiceDescriptor();
        addConfigureDescriptor(
            new ConfigureDescriptor(CONNECT_TYPE, false, ConfigurableServcieType.DEFAULT_SERVICE_NAME));
    }

    public static ConfigurableComponent getInstance(String namespace) {
        return ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
    }

    @Override
    protected boolean initProperties(Properties properties) {
        try {
            if (configureService != null) {
                return true;
            }
            this.configureService = ConfigurableServiceFactory.createConfigurableService(properties);
            return true;
        } catch (Exception e) {
            LOG.error("ConfigurableComponent create error,properties= " + properties, e);
            return false;
        }

    }

    @Override
    public boolean startComponent(String namespace) {
        try {
            this.namespace = namespace;
            configureService.initConfigurables(namespace);
            return true;
        } catch (Exception e) {
            LOG.error("ConfigurableComponent init error, namespace is " + namespace, e);
            return false;
        }

    }

    /**
     * 启动测试模式，用内存数据库存储和加载configurable数据
     */
    public static void begineTestMode() {
        System.setProperty(ConfigurableComponent.CONNECT_TYPE, ConfigurableServcieType.MEMORY_SERVICE_NAME);
    }

    /**
     * 关闭测试模式，用配置文件中配置的属性加载configuable数据
     */
    public static void endTestMode() {
        System.clearProperty(ConfigurableComponent.CONNECT_TYPE);
    }

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public IConfigurableService getService() {
        return configureService;
    }

    @Override
    public void initConfigurables(String namespace) {
        configureService.initConfigurables(namespace);
    }

    @Override
    public boolean refreshConfigurable(String namespace) {
        return configureService.refreshConfigurable(namespace);
    }

    public void mockConfigurable(String namespace) {
        refreshConfigurable(namespace);

    }

    @Override
    public List<IConfigurable> queryConfigurable(String type) {
        return configureService.queryConfigurable(type);
    }

    @Override
    public <T extends IConfigurable> List<T> queryConfigurableByType(String type) {
        return configureService.queryConfigurableByType(type);
    }

    @Override
    public IConfigurable queryConfigurableByIdent(String type, String name) {
        return configureService.queryConfigurableByIdent(type, name);
    }

    @Override
    public IConfigurable queryConfigurableByIdent(String identification) {
        return configureService.queryConfigurableByIdent(identification);
    }

    @Override
    public void insert(IConfigurable configurable) {
        configureService.insert(configurable);
        ConfigurableUtil.refreshMock(configurable);
    }

    @Override
    public void update(IConfigurable configurable) {
        configureService.update(configurable);
    }

    @Override
    public <T> Map<String, T> queryConfigurableMapByType(String type) {
        return configureService.queryConfigurableMapByType(type);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T queryConfigurable(String configurableType, String name) {
        return (T)queryConfigurableByIdent(configurableType, name);
    }

    //protected void insertConfigurable(JSONObject message, IConfigurable configurable) {
    //    ConfigurableUtil.insertConfigurable(message, configurable, this.configureService);
    //}

    @Override
    public String getNamespace() {
        if (AbstractConfigurableService.class.isInstance(configureService)) {
            return ((AbstractConfigurableService)configureService).getNamespace();
        }
        return namespace;
    }

    @Override
    public Collection<IConfigurable> findAll() {
        return configureService.findAll();
    }

    @Override
    public <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type) {
        return configureService.loadConfigurableFromStorage(type);
    }
}
