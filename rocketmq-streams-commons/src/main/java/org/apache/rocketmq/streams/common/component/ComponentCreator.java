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

import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PropertiesUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 创建组件，如果参数未发生变化（如果未传入，则是配置文件的参数），返回同一个组件对象，如果发生变化，返回不同的组件对象
 */
public class ComponentCreator {

    /**
     * 代理dbchannel的class，需要继承JDBCDataSource抽象类。如果配置这个参数，则会给dbchannel增加一层代理，所有需要db访问的，都是通过open api发送sql给代理
     */
    public static final String DB_PROXY_CLASS_NAME = "db_proxy_class_name";
    /**
     * 创建channel的服务
     */
    public static final String DIPPER_INSTANCE_CHANNEL_CREATOR_SERVICE_NAME = "dipper_instance_channel_creator_service_name";
    /**
     * blink jar包所在的路径
     */
    public static final String UDF_JAR_PATH = "dipper.udf.jar.path";
    public static final String UDF_JAR_OSS_ACCESS_ID = "dipper.udf.jar.oss.access.id";
    public static final String UDF_JAR_OSS_ACCESS_KEY = "dipper.udf.jar.oss.access.key";
    private static final Map<String, IComponent> key2Component = new HashMap<>();
    private static Properties properties;

    static {
        Properties tmpProperties = PropertiesUtil.getResourceProperties("dipper.properties");
        if (tmpProperties == null) {
            tmpProperties = new Properties();
        }
        ComponentCreator.properties = tmpProperties;
    }

    public static String getDBProxyClassName() {
        return properties.getProperty(DB_PROXY_CLASS_NAME);
    }

    public static String[] createKV(Properties properties) {
        List<String> keys = new ArrayList<>();
        for (Object o : properties.keySet()) {
            keys.add(o.toString());
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

    @Deprecated(since = "该方法直接改动了系统配置， 后续会下线")
    public static void createProperties(Properties properties) {
        ComponentCreator.properties = loadOtherProperty(properties);
    }

    @Deprecated(since = "该方法直接改动了系统配置， 后续会下线")
    public static void createProperties(String propertiesFilePath) {
        Properties properties = PropertiesUtil.getResourceProperties(propertiesFilePath);
        if (properties == null) {
            properties = PropertiesUtil.loadPropertyByFilePath(propertiesFilePath);
        }
        ComponentCreator.properties = loadOtherProperty(properties);
    }

    private static Properties loadOtherProperty(Properties tmp, String... kvs) {
        Properties tmpProperties = new Properties();
        for (Entry<Object, Object> entry : tmp.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            String realValue = value;
            if (value.contains("#{")) {
                realValue = SQLUtil.parseIbatisSQL(tmp, value, true);
                if (realValue != null && realValue.startsWith("'")) {
                    realValue = realValue.replace("'", "");
                }
            }

            tmpProperties.put(key, realValue);
        }
        if (kvs == null) {
            return tmpProperties;
        }
        for (String kv : kvs) {
            int startIndex = kv.indexOf(":");
            String key = kv.substring(0, startIndex);
            String value = kv.substring(startIndex + 1);
            tmpProperties.put(key, value);
        }
        tmpProperties.putAll(properties);
        return tmpProperties;
    }

    private static Properties loadOtherProperty(Properties tmp, Properties tmp1) {
        Properties tmpProperties = new Properties();
        for (Entry<Object, Object> entry : tmp.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            String realValue = value;
            if (value.contains("#{")) {
                realValue = SQLUtil.parseIbatisSQL(tmp, value, true);
                if (realValue != null && realValue.startsWith("'")) {
                    realValue = realValue.replace("'", "");
                }
            }
            tmpProperties.put(key, realValue);
        }
        if (tmp1 == null) {
            return tmpProperties;
        }
        for (Entry<Object, Object> entry : tmp1.entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            String realValue = value;
            if (value.contains("#{")) {
                realValue = SQLUtil.parseIbatisSQL(tmp, value, true);
                if (realValue != null && realValue.startsWith("'")) {
                    realValue = realValue.replace("'", "");
                }
            }
            tmpProperties.put(key, realValue);
        }
        tmpProperties.putAll(properties);
        return tmpProperties;
    }

    public static <T extends IComponent> T getComponent(String namespace, Class componentType, Properties properties) {
        Properties newProperties = loadOtherProperty(ComponentCreator.properties, properties);
        String[] kvArray = createKV(newProperties);
        return (T) getComponentInner(namespace, componentType, true, kvArray);
    }

    public static <T extends IComponent> T getComponent(String namespace, Class componentType, String... kvs) {
        return getComponent(namespace, componentType, true, kvs);
    }

    protected static <T extends IComponent> T getComponent(String namespace, Class componentType, boolean isStart, String... kvs) {
        Properties properties = loadOtherProperty(ComponentCreator.properties, kvs);
        String[] kvArray = createKV(properties);
        return (T) getComponentInner(namespace, componentType, isStart, kvArray);
    }

    public static <T extends IComponent> T getComponent(String namespace, Class componentType) {
        return (T) getComponent(namespace, componentType, true, createKV(properties));
    }

    public static <T extends IComponent> T getComponentNotStart(String namespace, Class componentType) {
        return (T) getComponentInner(namespace, componentType, false, createKV(properties));
    }

    public static <T extends IComponent> T getComponentNotStart(String namespace, Class componentType, Properties properties) {
        Properties newProperties = loadOtherProperty(ComponentCreator.properties, properties);
        String[] kvArray = createKV(newProperties);
        return (T) getComponentInner(namespace, componentType, false, kvArray);
    }

    public static <T extends IComponent> T getComponentNotStart(String namespace, Class componentType, String... kvs) {
        return getComponent(namespace, componentType, false, kvs);
    }

    @Deprecated public static <T extends IComponent> T getComponentUsingPropertiesFile(String namespace, Class componentType, String propertiesPath) {
        return (T) getComponentInner(namespace, componentType, true, propertiesPath);
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
                throw new RuntimeException("can not get component.  namespace is " + namespace + ", type is " + componentType.getName(), e);
            }
        }

    }

    private static String createKey(Class<IComponent> componentType, String namespace, Object o) {
        String key = MapKeyUtil.createKey(componentType.getName(), namespace);
        if (o == null) {
            return key;
        } else if (o instanceof String) {
            String propertiesPath = (String) o;
            return MapKeyUtil.createKey(key, propertiesPath);
        } else if (o.getClass().isArray()) {
            String[] properties = (String[]) o;
            String pk = MapKeyUtil.createKeyBySign("_", properties);
            String md5Pk = StringUtil.createMD5Str(pk);
            return MapKeyUtil.createKey(key, md5Pk);
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
            String propertiesPath = (String) o;
            URL url = PropertiesUtil.class.getClassLoader().getResource(propertiesPath);
            if (url != null) {
                component.initByClassPath(propertiesPath);
            } else {
                component.initByFilePath(propertiesPath);
            }
        } else if (o.getClass().isArray()) {
            component.initByPropertiesStr((String[]) o);
        }
        return;
    }

//    public static void createProperty(String outputFilePath) {
//        Properties properties = ComponentCreator.getProperties();
//        PropertiesUtil.flush(properties, outputFilePath);
//    }

//    public static Properties getPropertiesDirectly() {
//        return properties;
//    }
//
//    public static Properties getProperties() {
//        return SQLParseContext.getProperties();
//    }

    public static void setProperties(String propertiesPath) {
        createProperties(propertiesPath);
    }

    public static boolean getPropertyBooleanValue(String key) {
        String value = properties.getProperty(key);
        if (value == null) {
            return false;
        }
        return "true".equals(value);
    }
}
