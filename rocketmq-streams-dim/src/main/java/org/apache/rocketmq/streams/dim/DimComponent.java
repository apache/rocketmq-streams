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
package org.apache.rocketmq.streams.dim;

import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.dim.service.IDimService;
import org.apache.rocketmq.streams.dim.service.impl.DimServiceImpl;

public class DimComponent extends AbstractComponent<IDimService> {

    private static final Log LOG = LogFactory.getLog(DimComponent.class);

    // private transient Map<String, DBDim> nameListMap = new HashMap<>();

    protected transient ConfigurableComponent configurableComponent;
    private transient IDimService dimService;

    public static DimComponent getInstance(String namespace) {
        return ComponentCreator.getComponent(namespace, ComponentCreator.class);
    }

    @Override
    protected boolean startComponent(String namespace) {
        configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
        dimService = new DimServiceImpl(configurableComponent);
        return true;
    }

    @Override
    protected boolean initProperties(Properties properties) {
        return true;
    }

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public IDimService getService() {
        return dimService;
    }
}
