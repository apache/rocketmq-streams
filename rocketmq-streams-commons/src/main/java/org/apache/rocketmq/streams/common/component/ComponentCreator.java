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
package org.apache.rocketmq.streams.common.component;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PropertiesUtils;
import org.apache.rocketmq.streams.common.utils.SQLUtil;

import java.net.URL;
import java.util.*;
import java.util.Map.Entry;

/**
 * 创建组件，如果参数未发生变化（如果未传入，则是配置文件的参数），返回同一个组件对象，如果发生变化，返回不同的组件对象
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ComponentCreator {

    private static final Log LOG = LogFactory.getLog(ComponentCreator.class);

    /**
     * 代理dbchannel的class，需要继承JDBCDataSource抽象类。如果配置这个参数，则会给dbchannel增加一层代理，所有需要db访问的，都是通过open api发送sql给代理
     */
    public static final String DB_PROXY_CLASS_NAME = "db_proxy_class_name";

    /**
     * 创建channel的服务
     */
    public static final String DIPPER_INSTANCE_CHANNEL_CREATOR_SERVICE_NAME
        = "dipper_instance_channel_creator_service_name";

    /**
     * blink jar包所在的路径
     */
    public static final String BLINK_UDF_JAR_PATH = "dipper.blink.udf.jar.path";
    private static final Map<String, IComponent> key2Component = new HashMap<>();
    private static Properties properties;
    public static String propertiesPath;//属性文件位置，便于定期刷新

    static {
        Properties properties1 = PropertiesUtils.getResourceProperties("dipper.properties");
        if (properties1 == null) {
            ComponentCreator.createMemoryProperties(10000L);
        } else {
            ComponentCreator.setProperties(properties1);
        }
    }

    public static String getDBProxyClassName() {
        return properties.getProperty(DB_PROXY_CLASS_NAME);
    }

    public static void setProperties(String propertiesPath) {
        ComponentCreator.propertiesPath = propertiesPath;
        createProperties(propertiesPath);
    }

    public static void setProperties(Properties properties) {
        ComponentCreator.properties = properties;
    }

    public static String[] createKV(Properties properties) {
        List<String> keys = new ArrayList<>();
        Iterator<Object> keyIterator = properties.keySet().iterator();
        while (keyIterator.hasNext()) {
            keys.add(keyIterator.next().toString());
        }
        Collections.sort(keys);
        Iterator<String> it = keys.iterator();
        String[] kvs = new String[properties.size()];
        int i = 0;
        while (it.hasNext()) {
            String key = it.next();
            String value = properties.getProperty(key);
            kvs[i] = key + ":" + value;
            i++;
        }
        return kvs;
    }

    public static void createProperties(Properties properties) {
        ComponentCreator.properties = loadOtherProperty(properties);
    }

    public static void createDBProperties(String url, String userName, String password, String tableName, String driverClass) {
        Properties properties = new Properties();
        properties.put(AbstractComponent.JDBC_DRIVER, driverClass);
        properties.put(AbstractComponent.JDBC_URL, url);
        properties.put(AbstractComponent.JDBC_USERNAME, userName);
        properties.put(AbstractComponent.JDBC_PASSWORD, password);
        properties.put(AbstractComponent.JDBC_TABLE_NAME, tableName);
        properties.put(AbstractComponent.CONNECT_TYPE, IConfigurableService.DEFAULT_SERVICE_NAME);
        ComponentCreator.properties = loadOtherProperty(properties);
    }

    public static void createDBProperties(String url, String userName, String password, String tableName, Long pollingTime, String... kvs) {
        Properties properties = new Properties();
        properties.put(AbstractComponent.JDBC_DRIVER, AbstractComponent.DEFAULT_JDBC_DRIVER);
        properties.put(AbstractComponent.JDBC_URL, url);
        properties.put(AbstractComponent.JDBC_USERNAME, userName);
        properties.put(AbstractComponent.JDBC_PASSWORD, password);
        properties.put(AbstractComponent.JDBC_TABLE_NAME, tableName);
        properties.put(AbstractComponent.POLLING_TIME, pollingTime + "");
        properties.put(AbstractComponent.CONNECT_TYPE, IConfigurableService.DEFAULT_SERVICE_NAME);
        ComponentCreator.properties = loadOtherProperty(properties, kvs);
    }

    public static Properties createFileProperties(String filePathName, Long pollingTime, String... kvs) {
        Properties properties = new Properties();
        properties.put(AbstractComponent.CONNECT_TYPE, IConfigurableService.FILE_SERVICE_NAME);
        properties.put(IConfigurableService.FILE_PATH_NAME, filePathName);
        properties.put(AbstractComponent.POLLING_TIME, pollingTime + "");
        Properties properties1 = loadOtherProperty(properties, kvs);
        ComponentCreator.properties = properties1;
        return properties1;
    }

    public static void createMemoryProperties(Long pollingTime, String... kvs) {
        Properties properties = new Properties();
        properties.put(AbstractComponent.CONNECT_TYPE, IConfigurableService.MEMORY_SERVICE_NAME);
        properties.put(AbstractComponent.POLLING_TIME, pollingTime + "");
        ComponentCreator.properties = loadOtherProperty(properties, kvs);
    }

    public static void createProperties(String propertiesFilePath, String... kvs) {
        Properties properties = PropertiesUtils.getResourceProperties(propertiesFilePath);
        if (properties == null) {
            properties = PropertiesUtils.loadPropertyByFilePath(propertiesFilePath);
        }
        ComponentCreator.properties = loadOtherProperty(properties, kvs);
    }

    private static Properties loadOtherProperty(Properties tmp, String... kvs) {
        Properties properties = new Properties();
        for (Entry<Object, Object> entry : tmp.entrySet()) {
            String key = (String)entry.getKey();
            String value = (String)entry.getValue();
            String realValue = value;
            if (value.contains("#{")) {
                realValue = SQLUtil.parseIbatisSQL(tmp, value, true);
                if (realValue != null && realValue.startsWith("'")) {
                    realValue = realValue.replace("'", "");
                }
            }

            properties.put(key, realValue);
        }
        if (kvs == null || kvs.length == 0) {
            return properties;
        }
        for (String kv : kvs) {
            int startIndex = kv.indexOf(":");
            String key = kv.substring(0, startIndex);
            String value = kv.substring(startIndex + 1);
            properties.put(key, value);

        }
        return properties;
    }

    public static <T extends IComponent> T getComponent(String namespace, Class componentType, String... kvs) {
        return getComponent(namespace, componentType, true, kvs);
    }

    protected static <T extends IComponent> T getComponent(String namespace, Class componentType, boolean isStart,
                                                           String... kvs) {
        Properties properties = loadOtherProperty(ComponentCreator.properties, kvs);
        String[] kvArray = createKV(properties);
        return (T)getComponentInner(namespace, componentType, isStart, kvArray);
    }

    public static <T extends IComponent> T getComponent(String namespace, Class componentType) {
        return (T)getComponent(namespace, componentType, true, createKV(properties));
    }

    public static <T extends IComponent> T getComponentNotStart(String namespace, Class componentType) {
        return (T)getComponentInner(namespace, componentType, false, createKV(properties));
    }

    public static <T extends IComponent> T getComponentNotStart(String namespace, Class componentType, String... kvs) {
        return getComponent(namespace, componentType, false, kvs);
    }

    @Deprecated
    public static <T extends IComponent> T getComponentUsingPropertiesFile(String namespace, Class componentType,
                                                                           String propertiesPath) {
        return (T)getComponentInner(namespace, componentType, true, propertiesPath);
    }

    private static IComponent getComponentInner(String namespace, Class<IComponent> componentType, boolean needStart, Object o) {
        String key = createKey(componentType, namespace, o);
        if (key2Component.containsKey(key) && key2Component.get(key) != null) {
            return key2Component.get(key);
        }
        synchronized (ComponentCreator.class) {
            if (key2Component.containsKey(key) && key2Component.get(key) != null) {
                return key2Component.get(key);
            }
            try {
                IComponent component = componentType.newInstance();
                initComponent(component, o);
                key2Component.put(key, component);
                if (needStart) {
                    component.start(namespace);
                }

                return component;
            } catch (Exception e) {
                LOG.error("can not get component.  namespace is " + namespace + ", type is " + componentType.getName(),
                    e);
                throw new RuntimeException(e.getMessage(), e);
            }
        }

    }

    private static String createKey(Class<IComponent> componentType, String namespace, Object o) {
        String key = MapKeyUtil.createKey(componentType.getName(), namespace);
        if (o == null) {
            return key;
        } else if (o instanceof String) {
            String propertiesPath = (String)o;
            return MapKeyUtil.createKey(key, propertiesPath);
        } else if (o.getClass().isArray()) {
            String[] properties = (String[])o;
            String pk = MapKeyUtil.createKeyBySign("_", properties);
            return MapKeyUtil.createKey(key, pk);
        }
        return key;
    }

    /**
     * 初始化组件，支持多种初始化方法
     *
     * @param component
     * @param o
     */
    private static void initComponent(IComponent component, Object o) {
        if (o == null) {
            component.init();
        } else if (o instanceof String) {
            String propertiesPath = (String)o;
            URL url = PropertiesUtils.class.getClassLoader().getResource(propertiesPath);
            if (url != null) {
                component.initByClassPath(propertiesPath);
            } else {
                component.initByFilePath(propertiesPath);
            }
        } else if (o.getClass().isArray()) {
            component.initByPropertiesStr((String[])o);
        }
        return;
    }


    public static void createProperty(String outputFilePath) {
        Properties properties = ComponentCreator.getProperties();
        PropertiesUtils.flush(properties, outputFilePath);
    }

    public static Properties getProperties() {
        return properties;
    }

    public static boolean getPropertyBooleanValue(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            return false;
        }
        if ("true".equals(value)) {
            return true;
        }
        return false;
    }
}
