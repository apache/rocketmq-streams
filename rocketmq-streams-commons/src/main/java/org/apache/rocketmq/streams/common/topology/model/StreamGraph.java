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
package org.apache.rocketmq.streams.common.topology.model;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableQuery;
import org.apache.rocketmq.streams.common.configurable.IFieldProcessor;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.utils.ENVUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class StreamGraph extends BasedConfigurable implements IConfigurableQuery {

    protected List<IConfigurable> configurables;
    protected ChainPipeline mainPipeline;
    protected List<StreamGraph> subStreamGraphs;

    protected Map<String, List<IConfigurable>> type2Configurables = new HashMap<>();
    protected Map<String, IConfigurable> name2Configurables = new HashMap<>();

    public StreamGraph(ChainPipeline mainPipeline, List<IConfigurable> configurables) {
        this.mainPipeline = mainPipeline;
        this.configurables = configurables;
        this.setNameSpace(this.mainPipeline.getNameSpace());
        this.setName(this.mainPipeline.getName());
        for (IConfigurable configurable : configurables) {
            setENVVar(configurable);//替换环境变量
            configurable.init();//初始化对象
            registConfigurable(configurable);//实现IConfigurableQuery接口的数据结构

        }
//        executeAfterConfigurableRefreshListener(configurables);//实现IAfterConfigurableRefreshListener接口的回调

    }

    @Override public <T extends IConfigurable> List<T> queryConfigurableByType(String type) {
        List<T> result = new ArrayList<>();
        for (IConfigurable configurable : queryConfigurable(type)) {
            result.add((T) configurable);
        }
        return result;
    }

    @Override public <T> T queryConfigurable(String configurableType, String name) {
        return (T) name2Configurables.get(MapKeyUtil.createKey(configurableType, name));
    }

    @Override public List<IConfigurable> queryConfigurable(String type) {
        return type2Configurables.get(type);
    }

    @Override public <T> Map<String, T> queryConfigurableMapByType(String type) {
        Map<String, T> result = new HashMap<>();
        for (IConfigurable configurable : queryConfigurable(type)) {
            result.put(configurable.getName(), (T) configurable);
        }
        return result;
    }

    @Override public Collection<IConfigurable> findAll() {
        return this.configurables;
    }

    protected void setENVVar(IConfigurable configurable) {
        ReflectUtil.scanConfiguableFields(configurable, new IFieldProcessor() {
            @Override public void doProcess(Object o, Field field) {
                ENVDependence dependence = field.getAnnotation(ENVDependence.class);
                if (dependence == null) {
                    return;
                }
                String fieldValue = ReflectUtil.getBeanFieldValue(o, field.getName());
                if (fieldValue == null) {
                    return;
                }
                String value = getENVVar(fieldValue);
                if (StringUtil.isNotEmpty(value)) {
                    ReflectUtil.setBeanFieldValue(o, field.getName(), value);
                }
            }
        });
    }

    private void registConfigurable(IConfigurable configurable) {
        name2Configurables.put(MapKeyUtil.createKey(configurable.getType(), configurable.getName()), configurable);
        List<IConfigurable> configurableList = type2Configurables.get(configurable.getType());
        if (configurableList == null) {
            configurableList = new ArrayList<>();
            type2Configurables.put(configurable.getType(), configurableList);
        }
        configurableList.add(configurable);

    }

    protected String getENVVar(String fieldValue) {
        if (StringUtil.isEmpty(fieldValue)) {
            return null;
        }
        String value = SystemContext.getStringParameter(fieldValue);
        if (StringUtil.isNotEmpty(value)) {
            return value;
        }
        return ENVUtil.getENVParameter(fieldValue);
    }

    @Override public void destroy() {
        super.destroy();
        if (subStreamGraphs != null) {
            for (StreamGraph streamGraph : subStreamGraphs) {
                streamGraph.destroy();
            }
        }
        for (IConfigurable configurable : this.configurables) {
            configurable.destroy();
        }
    }

    public List<IConfigurable> getConfigurables() {
        return configurables;
    }

    public ChainPipeline getMainPipeline() {
        return mainPipeline;
    }

    public List<StreamGraph> getSubStreamGraphs() {
        return subStreamGraphs;
    }

    public void setSubStreamGraphs(List<StreamGraph> subStreamGraphs) {
        this.subStreamGraphs = subStreamGraphs;
    }
}
