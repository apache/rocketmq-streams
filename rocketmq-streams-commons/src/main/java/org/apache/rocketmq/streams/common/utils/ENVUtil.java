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

import java.util.Map;
import java.util.Properties;

public class ENVUtil {

    public static String getENVParameter(String key) {
        if (StringUtil.isEmpty(key)) {
            return null;
        }
        return System.getenv(key);
    }

    public static String getSystemParameter(String key) {
        return System.getProperty(key);
    }

    public static String getENVParameter(String key, Properties properties, boolean useENV) {
        if (!useENV) {
            return properties.getProperty(key);
        }
        String value = getENVParameter(key);
        if (StringUtil.isNotEmpty(value)) {
            return value;
        }
        return null;
    }

    public static void main(String[] args) {
        Map<String, String> map = System.getenv();
        for (String key : map.keySet()) {
            System.out.println(key + "=" + map.get(key));
        }
    }

}
