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
package org.apache.rocketmq.streams.common.monitor.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

//请用imonitor和dippermonitor
public class MonitorItem extends JSONObject {
    protected long startTime = System.currentTimeMillis();
    protected long endTime;
    protected volatile boolean success;
    protected Exception e;
    protected String name;
    protected int index;
    private JSONObject groupInfos = new JSONObject();
    private Set<String> noRepeate = new HashSet<>();

    public MonitorItem(String name) {
        this.name = name;
        this.put("name", name);
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
        this.put("index", index);
    }

    public MonitorItem startMonitor() {
        startTime = System.currentTimeMillis();
        return this;
    }

    public MonitorItem endMonitor() {
        endTime = System.currentTimeMillis();
        if (e != null) {
            return this;
        }
        this.success = true;
        this.put("cost", endTime - startTime);
        this.put("sucess", success);
        return this;
    }

    public long getCost() {
        return (Long) this.get("cost");
    }

    public MonitorItem occureError(Exception e, String... messages) {
        success = false;
        if (this.e != null && (e == null || "null".equals(e))) {
            return this;
        }
        this.e = e;
        System.out.println(e.getMessage());
        endTime = System.currentTimeMillis();
        this.put("cost", endTime - startTime);
        this.put("success", success);
        if (e != null) {
            this.put("exception", e.getMessage());
        }
        if (messages != null) {
            int i = 0;
            for (String message : messages) {
                this.put("errorMessage" + i, message);
                i++;
            }

        }
        return this;
    }

    public void addMessage(String key, JSONObject value) {
        this.put(key, value);
    }

    /**
     * 基于组增加日志，每组日志存储在一起
     *
     * @param groupName
     * @param key
     * @param value
     */
    public void addMessageByGroup(String groupName, String key, Object value) {
        JSONArray map = groupInfos.getJSONArray(groupName);
        if (map == null) {
            map = new JSONArray();
            groupInfos.put(groupName, map);
        }
        String repeateKey = MapKeyUtil.createKey(groupName, key);
        if (noRepeate.contains(repeateKey)) {
            return;
        }
        noRepeate.add(repeateKey);
        map.add(key + ":" + value);
    }

    /**
     * 输出组日志
     *
     * @param groupName
     */
    public void outputGroup(String groupName) {
        JSONArray map = groupInfos.getJSONArray(groupName);
        if (map == null) {
            return;
        }
        this.put(groupName, map);
    }

    public long getStartTime() {
        return startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Exception getE() {
        return e;
    }

    public void setE(Exception e) {
        this.e = e;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public JSONObject getGroupInfos() {
        return groupInfos;
    }
}
