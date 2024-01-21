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
package org.apache.rocketmq.streams.common.monitor;

import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.monitor.service.MonitorDataSyncService;
import org.apache.rocketmq.streams.common.monitor.service.impl.DBMonitorDataSyncImpl;
import org.apache.rocketmq.streams.common.monitor.service.impl.HttpMonitorDataSyncImpl;
import org.apache.rocketmq.streams.common.monitor.service.impl.RocketMQMonitorDataSyncImpl;

public class MonitorDataSyncServiceFactory {

    public static MonitorDataSyncService create() {
        String configureType = SystemContext.getStringParameter(ConfigurationKey.UPDATE_TYPE);
        if (DataSyncConstants.UPDATE_TYPE_DB.equalsIgnoreCase(configureType)) {
            return new DBMonitorDataSyncImpl();
        } else if (DataSyncConstants.UPDATE_TYPE_HTTP.equalsIgnoreCase(configureType)) {
            String accessId = SystemContext.getStringParameter(ConfigurationKey.HTTP_AK);
            String accessIdSecret = SystemContext.getStringParameter(ConfigurationKey.HTTP_SK);
            String endPoint = SystemContext.getStringParameter(ConfigurationKey.HTTP_SERVICE_ENDPOINT);
            return new HttpMonitorDataSyncImpl(accessId, accessIdSecret, endPoint);
        } else if (DataSyncConstants.UPDATE_TYPE_ROCKETMQ.equalsIgnoreCase(configureType)) {
            return new RocketMQMonitorDataSyncImpl();
        }

//        try {
//            Properties properties1 = new Properties();
//            properties1.putAll(properties);
//            String type = properties1.getProperty(CONFIGURABLE_SERVICE_TYPE);
//            if (StringUtil.isEmpty(type)) {
//                type = IConfigurableService.DEFAULT_SERVICE_NAME;
//                ;
//            }
//            IConfigurableService configurableService = getConfigurableServcieType(type);
//            if (AbstractSupportParentConfigureService.class.isInstance(configurableService)) {
//                ((AbstractSupportParentConfigureService)configurableService).initMethod(properties1);
//            }
//        } catch (Exception e) {
//            LOG.error("create ConfigurableService error", e);
//            return null;
//        }
        return null;
    }

}
