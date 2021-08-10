package org.apache.rocketmq.streams.client.strategy;

import org.apache.rocketmq.streams.common.configurable.IConfigurableService;

public enum ConfiguableConnector {

    DB(IConfigurableService.DEFAULT_SERVICE_NAME),MEMORY(IConfigurableService.MEMORY_SERVICE_NAME),FILE(IConfigurableService.FILE_SERVICE_NAME);
    private String name;
    private ConfiguableConnector(String name){
        this.name=name;
    }

    public String getName() {
        return name;
    }
}
