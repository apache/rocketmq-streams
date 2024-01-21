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
package org.apache.rocketmq.streams.common.configuration;

import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.utils.PropertiesUtil;

public class SystemContext {
    static MapDataType mapDataType = new MapDataType(new StringDataType(), new StringDataType());
    static DateDataType dateDataType = new DateDataType();
    private static Properties properties;

    static {
        Properties tmpProperties = PropertiesUtil.getResourceProperties("dipper.properties");
        if (tmpProperties == null) {
            tmpProperties = new Properties();
        }
        SystemContext.properties = tmpProperties;
    }

    public static Object getParameter(String key) {
        return properties.get(key);
    }

    public static void put(String key, Object value) {
        properties.put(key, value);
    }

    public static Properties getProperties() {
        return properties;
    }

    public static Integer getIntParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {
            return Integer.valueOf(value);
        }
        return null;
    }

    public static String getProperty(String key, String defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }

    public static String getProperty(String key) {
        return properties.getProperty(key);
    }

    public static String getStringParameter(String key) {
        return properties.getProperty(key);
    }

    public static Boolean getBooleanParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {
            return Boolean.valueOf(value);
        }
        return null;
    }

    public static Long getLongParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {
            return Long.valueOf(value);
        }
        return null;
    }

    public static Double getDoubleParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {
            return Double.valueOf(value);
        }
        return null;
    }

    public static Float getFloatParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {
            return Float.valueOf(value);
        }
        return null;
    }

    public static List<String> getListParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {
            String[] values = key.split(",");
            Arrays.asList(values);
        }
        return null;
    }

    public static Map<String, String> getMapParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {

            mapDataType.getData(value);
        }
        return null;

    }

    public static Date getDateParameter(String key) {
        String value = getStringParameter(key);
        if (value != null) {
            dateDataType.getData(value);
        }
        return null;
    }
}
