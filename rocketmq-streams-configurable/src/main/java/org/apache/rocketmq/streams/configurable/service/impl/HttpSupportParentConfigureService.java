package org.apache.rocketmq.streams.configurable.service.impl;

import com.google.auto.service.AutoService;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.configurable.service.AbstractSupportParentConfigureService;
import org.apache.rocketmq.streams.configurable.service.ConfigurableServcieType;


@AutoService(IConfigurableService.class)
@ServiceName(ConfigurableServcieType.HTTP_SERVICE_NAME)
public class HttpSupportParentConfigureService extends AbstractSupportParentConfigureService {

    private static final Log LOG = LogFactory.getLog(HttpConfigureService.class);


    public HttpSupportParentConfigureService() {

    }

    @Override
    protected void initBeforeInitConfigurable(Properties property) {
        parentConfigureService = new HttpConfigureService(properties);
        configureService = new HttpConfigureService(properties);

    }
}
