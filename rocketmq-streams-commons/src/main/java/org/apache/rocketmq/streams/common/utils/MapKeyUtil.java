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

import com.alibaba.fastjson.JSONObject;

import java.util.*;

public class MapKeyUtil {

    /**
     * 默认分隔符
     */
    private static final String SIGN = ";";

    public static String createKey(List<String> keyItems) {
        return createKey(SIGN, keyItems);
    }

    public static String createKey(String sign, List<String> keyItems) {
        return createKeyFromCollection(sign, keyItems);
    }

    public static String createKeyFromCollection(String sign, Collection<String> keyItems) {
        if (CollectionUtil.isEmpty(keyItems)) {
            return null;
        }
        if (StringUtil.isEmpty(sign)) {
            sign = SIGN;
        }
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (String keyItem : keyItems) {
            String value = keyItem;
            if (StringUtil.isEmpty(keyItem)) {
                value = "";
            }
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(sign);
            }
            sb.append(keyItem);
        }
        return sb.toString();
    }

    public static String createKey(String... keyItems) {
        return createKeyBySign(SIGN, keyItems);
    }

    public static String createKeyBySign(String sign, String... keyItems) {
        if (keyItems == null) {
            return null;
        }
        if (StringUtil.isEmpty(sign)) {
            sign = SIGN;
        }
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (String keyItem : keyItems) {
            if (StringUtil.isEmpty(keyItem)) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(sign);
            }
            sb.append(keyItem);
        }
        return sb.toString();
    }

    public static String[] splitKey(String key) {
        return key.split(SIGN);
    }

    public static String getFirst(String key){
        String[] keys = splitKey(key);
        return keys[0];
    }

    public static String getLast(String key){
        String[] keys = splitKey(key);
        assert keys.length >= 1 : "keys length must less than 1";
        return keys[keys.length - 1];
    }

    public static String getByIndex(String key, int index){
        String[] keys = splitKey(key);
        assert keys.length >= index : "index must less than length";
        return keys[index];
    }

    public static void main(String args[]) {
        Map<String, Object> map = parseMap("a:b,c:d,e:f");
        String value = toString(map);
        System.out.println(value);
        map = parseMap(value);
        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(map);
        PrintUtil.print(jsonObject);
    }

    public static String toString(Map<String, Object> map) {
        if (map == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
        boolean isFirst = true;
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String item = entry.getKey() + ":" + entry.getValue();
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }
            sb.append(item);
        }
        return sb.toString();
    }

    public static Map<String, Object> parseMap(String mapStr) {
        if (mapStr == null) {
            return null;
        }
        Map<String, Object> result = new HashMap<>();
        String tmp = mapStr;
        String[] items = tmp.split(",");
        for (String item : items) {
            String[] values = item.split(":");
            if (values.length == 2) {
                String key = values[0];
                String value = values[1];
                result.put(key, value);
            }
        }
        return result;
    }

    public static Map<String, String> parseMap(String... mapStrs) {
        if (mapStrs == null) {
            return null;
        }
        Map<String, String> result = new HashMap<>();
        for (String item : mapStrs) {
            String[] values = item.split(":");
            String key = values[0];
            String value = values[1];
            result.put(key, value);
        }
        return result;
    }

}
