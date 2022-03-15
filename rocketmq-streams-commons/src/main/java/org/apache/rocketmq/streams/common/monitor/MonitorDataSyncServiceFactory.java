package org.apache.rocketmq.streams.common.monitor;

import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.monitor.service.MonitorDataSyncService;
import org.apache.rocketmq.streams.common.monitor.service.impl.DBMonitorDataSyncImpl;
import org.apache.rocketmq.streams.common.monitor.service.impl.HttpMonitorDataSyncImpl;
import org.apache.rocketmq.streams.common.monitor.service.impl.RocketMQMonitorDataSyncImpl;

public class MonitorDataSyncServiceFactory {


    public static MonitorDataSyncService create() {
        String configureType = ComponentCreator.getProperties().getProperty(DataSyncConstants.UPDATE_TYPE);
        if (DataSyncConstants.UPDATE_TYPE_DB.equalsIgnoreCase(configureType)) {
            return new DBMonitorDataSyncImpl();
        } else if (DataSyncConstants.UPDATE_TYPE_HTTP.equalsIgnoreCase(configureType)) {
            String accessId = ComponentCreator.getProperties().getProperty(AbstractComponent.HTTP_AK);
            String accessIdSecret = ComponentCreator.getProperties().getProperty(AbstractComponent.HTTP_SK);
            String endPoint = ComponentCreator.getProperties().getProperty(IConfigurableService.HTTP_SERVICE_ENDPOINT);
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
