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

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.model.Configure;

public abstract class AbstractConfigurableService implements IConfigurableService {

    private static final Log LOG = LogFactory.getLog(AbstractConfigurableService.class);

    private static final String CLASS_NAME = IConfigurableService.CLASS_NAME;

    protected Map<String, List<IConfigurable>> type2ConfigurableMap = new HashMap<>();

    protected Map<String, IConfigurable> name2ConfigurableMap = new HashMap<>();

    protected Map<String, IConfigurable> configurableMap = new HashMap<>();

    protected Properties properties;

    protected transient String namespace;

    public AbstractConfigurableService(Properties properties) {
        this.properties = properties;
    }

    public AbstractConfigurableService() {
    }

    @Override
    public IConfigurable queryConfigurableByIdent(String identification) {
        return name2ConfigurableMap.get(identification);
    }

    protected String getConfigureKey(String nameSpace, String type, String name) {
        return MapKeyUtil.createKey(nameSpace, type, name);
    }

    protected void updateConfiguresCache(IConfigurable configurable) {
        if (configurable == null) {
            return;
        }
        configurable.toJson();
        String key = getConfigureKey(configurable.getNameSpace(), configurable.getType(), configurable.getConfigureName());
        configurableMap.put(key, configurable);
    }

    protected void updateConfiguresCache(List<IConfigurable> configureList) {
        for (IConfigurable iConfigurable : configureList) {
            updateConfiguresCache(iConfigurable);
        }
    }

    protected boolean equals(String key, List<?> newConfigureList) {
        for (Object o : newConfigureList) {
            IConfigurable configure = (IConfigurable)o;
            String tempKey = getConfigureKey(configure.getNameSpace(), configure.getType(), configure.getConfigureName());
            if (key.equals(tempKey)) {
                IConfigurable oldConfigure = configurableMap.get(key);
                if (oldConfigure == null) {
                    continue;
                }
                return ConfigurableUtil.compare(oldConfigure, configure);
            }
        }
        return false;
    }

    @Override
    public <T extends IConfigurable> List<T> queryConfigurableByType(String type) {
        List<IConfigurable> list = queryConfigurable(type);
        if (list == null) {
            return new ArrayList<T>();
        }
        List<T> result = new ArrayList<T>();
        for (IConfigurable configurable : list) {
            result.add((T)configurable);
        }
        return result;
    }

    @Override
    public boolean refreshConfigurable(String namespace) {
        //每次刷新，重新刷新配置文件
        //if(ComponentCreator.propertiesPath!=null){
        //    ComponentCreator.setProperties(ComponentCreator.propertiesPath);
        //}
        this.namespace = namespace;
        // Map<String, List<IConfigurable>> namespace2ConfigurableMap = new HashMap<>();
        Map<String, List<IConfigurable>> tempType2ConfigurableMap = new HashMap<>();
        Map<String, IConfigurable> tempName2ConfigurableMap = new HashMap<>();
        GetConfigureResult configures = loadConfigurable(namespace);
        // updateConfiguresCache(configures.getConfigure());
        if (configures != null && configures.isQuerySuccess() && configures.getConfigurables() != null) {
            // List<Configure> configureList = filterConfigure(configures.getConfigure());
            List<IConfigurable> configurables = configures.getConfigurables();
            List<IConfigurable> configurableList = checkAndUpdateConfigurables(configurables, tempType2ConfigurableMap, tempName2ConfigurableMap);
            // this.namespace2ConfigurableMap = namespace2ConfigurableMap;
            for (IConfigurable configurable : configurableList) {
                if (configurable instanceof IAfterConfigurableRefreshListener) {
                    ((IAfterConfigurableRefreshListener)configurable).doProcessAfterRefreshConfigurable(this);
                }
            }
            return true;
        }
        return false;
    }

    @Override
    public <T> T queryConfigurable(String configurableType, String name) {
        return (T)queryConfigurableByIdent(configurableType, name);
    }

    protected List<IConfigurable> checkAndUpdateConfigurables(List<IConfigurable> configurables, Map<String, List<IConfigurable>> tempType2ConfigurableMap, Map<String, IConfigurable> tempName2ConfigurableMap) {
        List<IConfigurable> configurableList = new ArrayList<>();
        for (IConfigurable configurable : configurables) {
            try {
                boolean isUpdate = update(configurable, tempName2ConfigurableMap, tempType2ConfigurableMap);
                if (isUpdate) {
                    configurableList.add(configurable);
                }
            } catch (Exception e) {
                LOG.error("组件初始化异常：" + e.getMessage() + ",name=" + configurable.getConfigureName(), e);
            }
        }
        destroyOldConfigurables(tempName2ConfigurableMap);
        this.name2ConfigurableMap = tempName2ConfigurableMap;
        this.type2ConfigurableMap = tempType2ConfigurableMap;
        return configurableList;
    }

    private void destroyOldConfigurables(Map<String, IConfigurable> tempName2ConfigurableMap) {
        Iterator<Map.Entry<String, IConfigurable>> it = this.name2ConfigurableMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, IConfigurable> entry = it.next();
            String key = entry.getKey();
            IConfigurable value = entry.getValue();
            if (!tempName2ConfigurableMap.containsKey(key)) {
                destroyOldConfigurable(value);
            }
        }

    }

    private void destroyOldConfigurable(IConfigurable oldConfigurable) {
        if (AbstractConfigurable.class.isInstance(oldConfigurable)) {
            ((AbstractConfigurable)oldConfigurable).destroy();
        }
        String key = getConfigureKey(oldConfigurable.getNameSpace(), oldConfigurable.getType(),
            oldConfigurable.getConfigureName());
        configurableMap.remove(key);
    }

    protected void initConfigurable(IConfigurable configurable) {
        if (AbstractConfigurable.class.isInstance(configurable)) {
            AbstractConfigurable abstractConfigurable = (AbstractConfigurable)configurable;
            abstractConfigurable.setConfigurableService(this);
        }

        configurable.init();

    }

    /**
     * 内部使用
     */
    private ScheduledExecutorService scheduledExecutorService;

    @Override
    public void initConfigurables(final String namespace) {
        refreshConfigurable(namespace);
        long polingTime = -1;
        if (this.properties != null) {
            String pollingTimeStr = this.properties.getProperty(AbstractComponent.POLLING_TIME);
            if (StringUtil.isNotEmpty(pollingTimeStr)) {
                polingTime = Long.valueOf(pollingTimeStr);
            }
        }
        if (polingTime > 0) {
            scheduledExecutorService = new ScheduledThreadPoolExecutor(3);
            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {

                @Override
                public void run() {
                    try {
                        refreshConfigurable(namespace);
                    } catch (Exception e) {
                        LOG.error("Load configurables error:" + e.getMessage(), e);
                    }
                }
            }, polingTime, polingTime, TimeUnit.SECONDS);
        }
    }
    // @Override
    // public List<IConfigurable> queryConfigurable(String nameSpace) {
    // return namespace2ConfigurableMap.get(nameSpace);
    // }

    @Override
    public List<IConfigurable> queryConfigurable(String type) {
        String key = MapKeyUtil.createKey(type);
        return type2ConfigurableMap.get(key);
    }

    @Override
    public IConfigurable queryConfigurableByIdent(String type, String name) {
        String key = MapKeyUtil.createKey(type, name);
        return name2ConfigurableMap.get(key);
    }

    /**
     * 根据namespace加载配置信息
     *
     * @param namespace
     * @return
     * @throws Exception
     */
    protected abstract GetConfigureResult loadConfigurable(String namespace);

    @Override
    public void update(IConfigurable configurable) {
        // update(configurable,name2ConfigurableMap,type2ConfigurableMap);
        updateConfigurable(configurable);
    }

    protected abstract void updateConfigurable(IConfigurable configurable);

    protected abstract void insertConfigurable(IConfigurable configurable);

    protected boolean update(IConfigurable configurable, Map<String, IConfigurable> name2ConfigurableMap,
        Map<String, List<IConfigurable>> type2ConfigurableMap) {
        if (configurable == null) {
            return false;
        }

        boolean isUpdate = false;
        List<IConfigurable> configurableList = new ArrayList<>();
        configurableList.add(configurable);

        String nameKey = MapKeyUtil.createKey(configurable.getType(), configurable.getConfigureName());
        if (this.name2ConfigurableMap.containsKey(nameKey)) {
            String configureKey = getConfigureKey(namespace, configurable.getType(), configurable.getConfigureName());
            IConfigurable oldConfigurable = this.name2ConfigurableMap.get(nameKey);
            if (equals(configureKey, configurableList)) {
                configurable = oldConfigurable;
                // name2ConfigurableMap.put(nameKey, name2ConfigurableMap.get(nameKey));
            } else {
                destroyOldConfigurable(oldConfigurable);
                initConfigurable(configurable);
                isUpdate = true;
            }
        } else {
            initConfigurable(configurable);
            isUpdate = true;
        }
        updateConfiguresCache(configurable);
        name2ConfigurableMap.put(nameKey, configurable);
        String typeKey = MapKeyUtil.createKey(configurable.getType());
        // put2Map(namespace2ConfigurableMap, namespace, configurable);
        put2Map(type2ConfigurableMap, typeKey, configurable);
        return isUpdate;
    }

    @Override
    public void insert(IConfigurable configurable) {
        // update(configurable,name2ConfigurableMap,type2ConfigurableMap);
        insertConfigurable(configurable);
    }

    /**
     * 给一个扣，可以跨命名空间查询数据
     *
     * @param namespaces
     * @return
     */
    public List<IConfigurable> queryConfiguableByNamespace(String... namespaces) {
        List<IConfigurable> configurables = new ArrayList<>();
        if (namespaces == null || namespaces.length == 0) {
            return configurables;
        }
        for (String namespace : namespaces) {
            GetConfigureResult result = loadConfigurable(namespace);
            if (result.querySuccess) {
                if (result.configurables != null && result.configurables.size() > 0) {
                    configurables.addAll(result.configurables);
                }
            } else {
                throw new RuntimeException("Load configurable error, the namespace is " + namespace);
            }
        }
        return configurables;

    }

    /**
     * 往一个value是list的map中添加数据，如果list是空创建，否则直接插入
     *
     * @param map
     * @param key
     * @param configurable
     */
    protected void put2Map(Map<String, List<IConfigurable>> map, String key, IConfigurable configurable) {
        List<IConfigurable> list = map.get(key);
        if (list == null) {
            list = new ArrayList<IConfigurable>();
            map.put(key, list);
        }
        list.add(configurable);
    }

    @Override
    public Collection<IConfigurable> findAll() {
        return name2ConfigurableMap.values();
    }

    /**
     * 把configurable转换成configure
     *
     * @param configurable
     * @return
     */
    protected Configure createConfigure(IConfigurable configurable) {
        Configure configure = new Configure();
        configure.setType(configurable.getType());
        configure.setName(configurable.getConfigureName());
        configure.setNameSpace(configurable.getNameSpace());
        String jsonString = configurable.toJson();
        if (!StringUtil.isEmpty(jsonString)) {
            JSONObject jsonObject = JSONObject.parseObject(jsonString);
            jsonObject.put(CLASS_NAME, configurable.getClass().getName());
            configure.setJsonValue(jsonObject.toJSONString());
        }
        // configure.createIdentification();
        return configure;
    }

    @Override
    public <T> Map<String, T> queryConfigurableMapByType(String type) {
        List<IConfigurable> configurables = queryConfigurable(type);
        if (configurables == null) {
            return new HashMap<String, T>();
        }
        Map<String, T> result = new HashMap<String, T>();
        for (IConfigurable configurable : configurables) {
            result.put(configurable.getConfigureName(), (T)configurable);
        }
        return result;
    }

    /**
     * 把configure转换成configurable
     *
     * @param configures
     * @return
     */
    protected List<IConfigurable> convert(List<Configure> configures) {
        if (configures == null) {
            return new ArrayList<IConfigurable>();
        }
        List<IConfigurable> configurables = new ArrayList<IConfigurable>();
        for (Configure configure : configures) {
            IConfigurable configurable = convert(configure);
            if (configurable != null) {
                configurables.add(configurable);
            }

        }
        return configurables;
    }

    protected IConfigurable createConfigurableFromJson(String namespace, String type, String name, String jsonValue) {
        if (StringUtil.isEmpty(jsonValue)) {
            return null;
        }
        JSONObject jsonObject = JSONObject.parseObject(jsonValue);
        String className = jsonObject.getString(CLASS_NAME);
        IConfigurable configurable = createConfigurable(className);
        if (configurable == null) {
            return null;
        }
        configurable.setConfigureName(name);
        configurable.setNameSpace(namespace);
        configurable.setType(type);
        if (AbstractConfigurable.class.isInstance(configurable)) {
            AbstractConfigurable abstractConfigurable = (AbstractConfigurable)configurable;
            abstractConfigurable.setConfigurableService(this);
        }
        configurable.toObject(jsonValue);
        return configurable;
    }

    /**
     * 提供一个入口，可以让外部用户改变configure对应的configurable的值
     *
     * @param configure
     * @return
     */
    protected IConfigurable convert(Configure configure) {

        return convertConfigurable(configure);
    }

    protected IConfigurable convertConfigurable(Configure configure) {
        String className = null;
        try {
            String jsonString = configure.getJsonValue();
            IConfigurable configurable =
                createConfigurableFromJson(configure.getNameSpace(), configure.getType(), configure.getName(),
                    jsonString);
            if (configurable instanceof Entity) {
                // add by wangtl 20171110 Configurable接口第三方包也在用，故不能Configurable里加接口，只能加到抽象类里，这里强转下
                Entity abs = (Entity)configurable;
                abs.setId(configure.getId());
                abs.setGmtCreate(configure.getGmtCreate());
                abs.setGmtModified(configure.getGmtModified());
                /*
                 * abs.setTempKey((configurable.getNameSpace() + configurable.getType() +
                 * configurable.getConfigureName() + jsonString).hashCode());
                 */
            }
            convertPost(configurable);
            return configurable;
        } catch (Exception e) {
            LOG.error("转换异常：" + configure.toString(), e);
            return null;
        }
    }

    /**
     * 如果需要改变configurable的属性，可以再这里设置
     *
     * @param configurable
     */
    @SuppressWarnings("rawtypes")
    protected void convertPost(IConfigurable configurable) {
        if (this.properties == null) {
            return;
        }
        String identification =
            MapKeyUtil.createKey(configurable.getNameSpace(), configurable.getType(), configurable.getConfigureName());
        String propertyValue = this.properties.getProperty(identification);
        if (StringUtil.isEmpty(propertyValue)) {
            return;
        }
        String[] fieldName2Values = propertyValue.split(",");
        if (fieldName2Values == null || fieldName2Values.length == 0) {
            return;
        }
        for (String fieldName2Value : fieldName2Values) {
            try {
                String[] fieldName2ValueArray = fieldName2Value.split(":");
                if (fieldName2ValueArray == null || fieldName2ValueArray.length != 2) {
                    continue;
                }
                String fieldName = fieldName2ValueArray[0];
                String value = fieldName2ValueArray[1];
                Class clazz = ReflectUtil.getBeanFieldType(configurable.getClass(), fieldName);
                DataType dataType = DataTypeUtil.createDataType(clazz, null);
                if (dataType == null) {
                    continue;
                }
                Object fieldValue = dataType.getData(value);
                ReflectUtil.setBeanFieldValue(configurable, fieldName, fieldValue);

            } catch (Exception e) {
                LOG.error("convert post error " + fieldName2Value, e);
                continue;
            }

        }
    }

    /**
     * 创建configurable对象
     *
     * @param className class name
     * @return
     */
    @SuppressWarnings("rawtypes")
    protected IConfigurable createConfigurable(String className) {
        return ReflectUtil.forInstance(className);
    }

    public class GetConfigureResult {

        private boolean querySuccess;
        private List<IConfigurable> configurables;

        public boolean isQuerySuccess() {
            return querySuccess;
        }

        public void setQuerySuccess(boolean querySuccess) {
            this.querySuccess = querySuccess;
        }

        public List<IConfigurable> getConfigurables() {
            return configurables;
        }

        public void setConfigurables(List<IConfigurable> configurables) {
            this.configurables = configurables;
        }
    }

    @Override
    public String getNamespace() {
        return namespace;
    }

    public Properties getProperties() {
        return properties;
    }

    public void setProperties(Properties properties) {
        this.properties = properties;
    }
}
