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
package org.apache.rocketmq.streams.configurable.service;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

public class ConfigurableServiceFactory {
    private static ServiceLoaderComponent<IConfigurableService> configurableServiceLoaderComponent = ServiceLoaderComponent.getInstance(IConfigurableService.class);
    public static final String CONFIGURABLE_SERVICE_TYPE = "dipper.configurable.service.type";
    private static final Log LOG = LogFactory.getLog(ConfigurableServiceFactory.class);

    public static IConfigurableService createConfigurableService(Properties properties) {
        try {
            Properties properties1 = new Properties();
            properties1.putAll(properties);
            String type = properties1.getProperty(CONFIGURABLE_SERVICE_TYPE);
            if (StringUtil.isEmpty(type)) {
                type = IConfigurableService.MEMORY_SERVICE_NAME;
            }
            IConfigurableService configurableService = getConfigurableServiceType(type);
            if (configurableService instanceof AbstractSupportParentConfigureService) {
                ((AbstractSupportParentConfigureService)configurableService).initMethod(properties1);
            }
            return configurableService;
        } catch (Exception e) {
            LOG.error("create ConfigurableService error", e);
            return null;
        }

    }

    public static IConfigurableService getConfigurableServiceType(String type) {
        IConfigurableService configurableService = configurableServiceLoaderComponent.getService().loadService(type);
        return ReflectUtil.forInstance(configurableService.getClass().getName());
    }
}
