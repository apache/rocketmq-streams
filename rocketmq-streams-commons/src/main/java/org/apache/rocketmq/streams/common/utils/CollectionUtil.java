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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings("rawtypes")
public final class CollectionUtil {

    public static boolean isEmpty(Map map) {
        return map == null || map.isEmpty();
    }

    public static boolean isNotEmpty(Map map) {
        return map != null && !map.isEmpty();
    }

    public static boolean isEmpty(Collection list) {
        return list == null || list.isEmpty();
    }

    public static boolean isNotEmpty(Collection list) {
        return list != null && !list.isEmpty();
    }

    public static boolean isEmpty(Object[] array) {
        return array == null || array.length == 0;
    }

    public static boolean isNotEmpty(Object[] array) {
        return array != null && array.length > 0;
    }

    public static <T> List<T> asList(T... array) {
        if (array == null) {
            return null;
        }

        List<T> list = new ArrayList<>();
        for (T t : array) {
            if (t != null) {
                list.add(t);
            }
        }
        return list;
    }

    public static <T> void put2Set(Map<String, Set<T>> map, String key, T value) {
        Set<T> set = map.get(key);
        if (set == null) {
            set = new HashSet<T>();
            map.put(key, set);
        }
        set.add(value);
    }

    public static <T> void put2List(Map<String, List<T>> map, String key, T value) {
        List<T> set = map.get(key);
        if (set == null) {
            set = new ArrayList<>();
            map.put(key, set);
        }
        set.add(value);
    }

}
