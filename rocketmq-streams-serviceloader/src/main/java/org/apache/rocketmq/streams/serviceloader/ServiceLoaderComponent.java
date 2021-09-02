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
package org.apache.rocketmq.streams.serviceloader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.serviceloader.namefinder.IServiceNameGetter;

public class ServiceLoaderComponent<T> extends AbstractComponent<IServiceLoaderService<T>>
    implements IServiceLoaderService<T> {
    private Properties properties;
    private Class<T> interfaceClass;
    private Map<String, T> name2Service = new HashMap<>();
    private List<T> serviceList = new ArrayList<T>();
    private boolean hasRefresh = false;
    private boolean needServieName = true;

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public IServiceLoaderService<T> getService() {
        return this;
    }

    public static ServiceLoaderComponent getInstance(Class interfaceClass) {
        ServiceLoaderComponent serviceLoaderComponent =
            ComponentCreator.getComponent(interfaceClass.getName(), ServiceLoaderComponent.class);
        return serviceLoaderComponent;
    }

    @Override
    protected boolean startComponent(String interfaceClassName) {
        try {
            Class clazz = Class.forName(interfaceClassName);
            this.interfaceClass = clazz;
            refresh(false);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("class not found " + interfaceClassName, e);
        }
        return true;
    }

    @Override
    protected boolean initProperties(Properties properties) {
        this.properties = properties;
        return true;
    }

    @Override
    public T loadService(String serviceName) {
        if (!this.hasRefresh) {
            refresh(false);
        }
        return (T)this.name2Service.get(serviceName);
    }

    @Override
    public List<T> loadService() {
        if (!this.hasRefresh) {
            refresh(false);
        }
        return serviceList;
    }

    @Override
    public void refresh(boolean forceRefresh) {
        if (!forceRefresh && hasRefresh) {
            return;
        }
        synchronized (this) {
            if (!forceRefresh && hasRefresh) {
                return;
            }
            Map<String, T> name2Service = new HashMap<>();
            Iterable<T> iterable = ServiceLoader.load(interfaceClass);
            List<T> allService = new ArrayList<>();
            for (T t : iterable) {
                if (needServieName) {
                    List<String> serviceNames = loadServiceName(t);
                    if (serviceNames == null) {
                        name2Service.put(t.getClass().getSimpleName(), t);
                    } else {
                        for (String serviceName : serviceNames) {
                            name2Service.put(serviceName, t);
                        }
                    }

                }
                allService.add(t);
            }
            this.name2Service = name2Service;
            this.hasRefresh = true;
            this.serviceList = allService;
        }
    }

    static ServiceLoaderComponent nameLoaderComponent = new ServiceLoaderComponent();

    static {
        nameLoaderComponent.init();
        nameLoaderComponent.needServieName = true;
        nameLoaderComponent.startComponent(IServiceNameGetter.class.getName());
    }

    protected List<String> loadServiceName(T t) {
        List<String> serviceNames = new ArrayList();
        Class tClass = t.getClass();
        String serviceName = properties.getProperty(tClass.getName());
        if (properties != null && StringUtil.isNotEmpty(serviceName)) {

            serviceNames.add(serviceName);
            return serviceNames;
        }
        ServiceName annotation = (ServiceName)tClass.getAnnotation(ServiceName.class);
        if (annotation == null) {
            return null;
        }
        if (StringUtil.isNotEmpty(annotation.value())) {
            serviceNames.add(annotation.value());
        }
        if (StringUtil.isNotEmpty(annotation.aliasName())) {
            serviceNames.add(annotation.aliasName());
        }

        if (StringUtil.isNotEmpty(annotation.name())) {
            serviceNames.add(annotation.name());
        }

        return serviceNames;
    }
}
