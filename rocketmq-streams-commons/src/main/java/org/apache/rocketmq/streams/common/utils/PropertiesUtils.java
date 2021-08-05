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
package org.apache.rocketmq.streams.common.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PropertiesUtils {
    private static final Log LOG = LogFactory.getLog(PropertiesUtils.class);

    public static Properties getDefaultComponentProperties(Class clazz) {
        return getResourceProperties(getComponentPropertyPath(clazz));
    }

    public static Properties loadPropertyByFilePath(String filePath) {
        try {
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            Properties properties = new Properties();
            properties.load(br);
            return properties;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

    }

    public static void putProperty(String line, String sign, Map properties) {
        int startIndex = line.indexOf(sign);
        String key = line.substring(0, startIndex);
        String value = line.substring(startIndex + sign.length());

        properties.put(key, value);
    }

    /**
     * 加载指定位置的属性
     *
     * @param propertiesPath
     * @return
     */
    public static Properties getResourceProperties(Class clazz, String propertiesPath) {
        URL url = clazz.getClassLoader().getResource(propertiesPath);
        if (url == null) {
            LOG.error("can not load component's properties file " + propertiesPath);
            return null;
        }
        BufferedReader br = null;
        try {
            Properties properties = new Properties();
            br = new BufferedReader(new InputStreamReader(url.openStream()));
            properties.load(br);
            return properties;
        } catch (IOException e) {
            LOG.error("load component properties error " + propertiesPath);
            return null;
        } finally {
            try {
                br.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 加载指定位置的属性
     *
     * @param propertiesPath
     * @return
     */
    public static Properties getResourceProperties(String propertiesPath) {
        return getResourceProperties(PropertiesUtils.class, propertiesPath);
    }

    public static String getComponentPropertyPath(Class clazz) {
        return getComponentPath(clazz, "properties");
    }

    public static void print(Properties properties) {
        Iterator<Map.Entry<Object, Object>> it = properties.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Object, Object> entry = it.next();
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }

    public static String getComponentSpringXmlPath(Class clazz) {
        return getComponentPath(clazz, "xml");
    }

    /**
     * 获取组件属性文件默认的相对路径及名称
     *
     * @param
     * @return
     */
    private static String getComponentPath(Class clazz, String rear) {
        String filePath = "component/" + clazz.getSimpleName();
        String propertyFilePath = filePath + "." + rear;
        return propertyFilePath;
    }

    public static void flush(Properties properties, String outputFilePath) {
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new FileWriter(outputFilePath));
            properties.store(bw, null);
            bw.flush();
            ;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                ;
            }
        }
    }
}
