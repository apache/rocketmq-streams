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

package org.apache.rocketmq.streams.configuable.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.configuable.service.AbstractConfigurableService;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

public class MemoryConfigureService extends AbstractConfigurableService {

    private static Map<String, List<IConfigurable>> namespace2Configure = new HashMap<>();

    public MemoryConfigureService(Properties properties) {
        super(properties);
    }

    @Override
    protected GetConfigureResult loadConfigurable(String namespace) {
        GetConfigureResult result = new GetConfigureResult();
        result.setQuerySuccess(true);
        List<IConfigurable> configurableList = new ArrayList<>();
        List<IConfigurable> configurables = namespace2Configure.get(namespace);
        if (configurables == null) {
            configurableList = null;
        } else {
            List<IConfigurable> tmps = new ArrayList<>();
            tmps.addAll(configurables);
            for (IConfigurable configurable : tmps) {
                IConfigurable tmp = ReflectUtil.forInstance(configurable.getClass());
                tmp.toObject(configurable.toJson());
                tmp.setNameSpace(configurable.getNameSpace());
                tmp.setConfigureName(configurable.getConfigureName());
                configurableList.add(tmp);
            }
        }
        result.setConfigurables(configurableList);
        return result;
    }

    @Override
    protected void insertConfigurable(IConfigurable configurable) {
        if (configurable == null) {
            return;
        }

        String namespace = configurable.getNameSpace();
        List<IConfigurable> list = namespace2Configure.get(namespace);
        if (list == null) {
            synchronized (this) {
                list = namespace2Configure.get(namespace);
                if (list == null) {
                    list = new ArrayList<>();
                    namespace2Configure.put(namespace, list);
                }
            }
        }
        int removeIndex = -1;
        for (int i = 0; i < list.size(); i++) {
            IConfigurable config = list.get(i);
            if (config.getType().equals(configurable.getType()) && config.getConfigureName()
                    .equals(configurable.getConfigureName())) {
                removeIndex = i;
            }
        }
        if (AbstractConfigurable.class.isInstance(configurable)) {
            ((AbstractConfigurable)configurable).setConfigurableService(this);
        }
        if (removeIndex != -1) {
            list.remove(removeIndex);
        }
        list.add(configurable);
    }

    @Override
    protected void updateConfigurable(IConfigurable configure) {
        List<IConfigurable> list = namespace2Configure.get(configure.getNameSpace());
        if (list == null || list.size() == 0) {
            throw new RuntimeException(
                    "not have exist configure " + configure.getNameSpace() + "," + configure.getType() + ","
                            + configure.getConfigureName());
        }
        for (int i = 0; i < list.size(); i++) {
            IConfigurable config = list.get(i);
            if (config.getType().equals(configure.getType()) && config.getConfigureName()
                    .equals(configure.getConfigureName())) {
                list.set(i, configure);
                return;
            }
        }
        throw new RuntimeException(
                "not have exist configure " + configure.getNameSpace() + "," + configure.getType() + ","
                        + configure.getConfigureName());
    }

    @Override
    public <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type) {
        refreshConfigurable(getNamespace());
        return queryConfigurableByType(type);
    }
}
