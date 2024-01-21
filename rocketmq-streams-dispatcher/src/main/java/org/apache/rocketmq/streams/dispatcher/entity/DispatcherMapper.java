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
package org.apache.rocketmq.streams.dispatcher.entity;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.rocketmq.streams.dispatcher.IMapper;

public class DispatcherMapper implements IMapper<String> {

    private Map<String, TreeSet<String>> mapping;

    public DispatcherMapper() {
        this.mapping = Maps.newHashMap();
    }

    public DispatcherMapper(Map<String, TreeSet<String>> mapping) {
        this.mapping = mapping;
    }

    public static DispatcherMapper parse(String mapperString) {
        if (mapperString == null || mapperString.isEmpty()) {
            return null;
        }
        return new DispatcherMapper(JSONObject.parseObject(mapperString, new TypeReference<Map<String, TreeSet<String>>>() {
        }));
    }

    public static void main(String[] args) {
        Map<String, String> map1 = Maps.newHashMap();
        map1.put("key1", "value1");
        map1.put("key2", "value2");
        map1.put("key3", "value3");

        Map<String, String> map2 = Maps.newHashMap();
        map2.put("key1", "value1");
        map2.put("key3", "value3");
        map2.put("key2", "value2");

        System.out.println(map1.equals(map2));

        TreeSet<String> list1 = Sets.newTreeSet();
        list1.add("value3");
        list1.add("value2");
        list1.add("value1");

        System.out.println(list1.iterator().next());
        for (String s : list1) {
            System.out.println(s);
        }

        TreeSet<String> list2 = Sets.newTreeSet();
        list2.add("value3");
        list2.add("value1");
        list2.add("value2");

        System.out.println(list1.equals(list2));

        TreeSet<String> list3 = Sets.newTreeSet();
        list3.add("value2");
        list3.add("value1");
        list3.add("value3");

        TreeSet<String> list4 = Sets.newTreeSet();
        list4.add("value1");
        list4.add("value3");
        list4.add("value2");

        Map<String, TreeSet<String>> map3 = Maps.newHashMap();
        map3.put("key2", list2);
        map3.put("key1", list1);

        Map<String, TreeSet<String>> map4 = Maps.newHashMap();
        map4.put("key2", list4);
        map4.put("key1", list3);

        System.out.println(map3.equals(map4));

        DispatcherMapper mapper = new DispatcherMapper(map3);
        String json = mapper.toString();

        System.out.println(json);

        DispatcherMapper mapper1 = DispatcherMapper.parse(json);
        try {
            Set<String> tasks = mapper1.getTasks("key2");
            for (String task : tasks) {
                System.out.println(task);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public Map<String, TreeSet<String>> getMapping() {
        return mapping;
    }

    public void setMapping(Map<String, TreeSet<String>> mapping) {
        this.mapping = mapping;
    }

    @Override public Set<String> getInstances() throws Exception {
        return this.mapping.keySet();
    }

    @Override public Set<String> getTasks(String instance) throws Exception {
        return mapping.get(instance) == null ? Sets.newTreeSet() : mapping.get(instance);
    }

    @Override public Set<String> getTasks() throws Exception {
        Set<String> tasks = Sets.newTreeSet();
        for (Set<String> sets : mapping.values()) {
            tasks.addAll(sets);
        }
        return tasks;
    }

    @Override public void putTask(String instance, String task) throws Exception {
        TreeSet<String> tasks = mapping.get(instance);
        if (tasks == null) {
            tasks = Sets.newTreeSet();
        }
        if (task != null) {
            tasks.add(task);
        }
        mapping.put(instance, tasks);
    }

    @Override public void removeTask(String instance, String task) throws Exception {
        TreeSet<String> tasks = mapping.get(instance);
        tasks.remove(task);
    }

    @Override public void remove(String instance) throws Exception {
        mapping.remove(instance);
    }

    @Override public boolean containTask(String task) throws Exception {
        for (Map.Entry<String, TreeSet<String>> entry : this.mapping.entrySet()) {
            if (entry.getValue().contains(task)) {
                return true;
            }
        }
        return false;
    }

    @Override public boolean equals(Object obj) {
        if (obj instanceof DispatcherMapper) {
            DispatcherMapper mapper = (DispatcherMapper) obj;
            return this.mapping.equals(mapper.mapping);
        }
        return super.equals(obj);
    }

    @Override public String toString() {
        return JSONObject.toJSONString(this.mapping);
    }

}
