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
package org.apache.rocketmq.streams.lease;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.component.ConfigureDescriptor;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.service.ConfigurableServcieType;
import org.apache.rocketmq.streams.lease.service.ILeaseService;
import org.apache.rocketmq.streams.lease.service.ILeaseStorage;
import org.apache.rocketmq.streams.lease.service.impl.LeaseServiceImpl;
import org.apache.rocketmq.streams.lease.service.impl.MockLeaseImpl;
import org.apache.rocketmq.streams.lease.service.storages.DBLeaseStorage;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

import java.util.Properties;

/**
 * 通过db实现租约和锁，可以更轻量级，减少其他中间件的依赖 使用主备场景，只有一个实例运行，当当前实例挂掉，在一定时间内，会被其他实例接手 也可以用于全局锁
 *
 * @date 1/9/19
 */
public class LeaseComponent extends AbstractComponent<ILeaseService> {

    private static LeaseComponent leaseComponent = null;
    private static final Log LOG = LogFactory.getLog(LeaseComponent.class);
    private ILeaseService leaseService;

    public LeaseComponent() {
        initConfigurableServiceDescriptor();
        addConfigureDescriptor(
            new ConfigureDescriptor(CONNECT_TYPE, false, ConfigurableServcieType.DEFAULT_SERVICE_NAME));
    }

    public static LeaseComponent getInstance() {
        if (leaseComponent == null) {
            synchronized (LeaseComponent.class) {
                if (leaseComponent == null) {
                    leaseComponent = ComponentCreator.getComponent(null, LeaseComponent.class);
                }
            }
        }
        return leaseComponent;
    }

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public ILeaseService getService() {
        return leaseService;
    }

    @Override
    protected boolean startComponent(String namespace) {
        return true;
    }

    @Override
    protected boolean initProperties(Properties properties) {
        String connectType = properties.getProperty(JDBC_URL);
        if (StringUtil.isEmpty(connectType)) {
            this.leaseService = new MockLeaseImpl();
            return true;
        }

        LeaseServiceImpl leaseService = new LeaseServiceImpl();
        String storageName = ComponentCreator.getProperties().getProperty(ConfigureFileKey.LEASE_STORAGE_NAME);
        ILeaseStorage storasge = null;
        if (StringUtil.isEmpty(storageName)) {
            String jdbc = properties.getProperty(AbstractComponent.JDBC_DRIVER);
            String url = properties.getProperty(AbstractComponent.JDBC_URL);
            String userName = properties.getProperty(AbstractComponent.JDBC_USERNAME);
            String password = properties.getProperty(AbstractComponent.JDBC_PASSWORD);
            storasge = new DBLeaseStorage(jdbc, url, userName, password);
        } else {
            storasge = (ILeaseStorage)ServiceLoaderComponent.getInstance(ILeaseStorage.class).loadService(storageName);
        }
        leaseService.setLeaseStorage(storasge);
        this.leaseService = leaseService;
        return true;
    }
}
