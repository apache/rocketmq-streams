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

import com.alibaba.fastjson.JSONObject;

//请用imonitor和dippermonitor
public class NothingMonitorItem extends MonitorItem {

    public NothingMonitorItem(String name) {
        super(name);
    }

    public NothingMonitorItem startMonitor(String name) {
        ;
        return this;
    }

    @Override
    public NothingMonitorItem endMonitor() {

        return this;
    }

    public NothingMonitorItem occureError(Exception e) {
        return this;
    }

    public void addMessage(String key, String value) {

    }

    public static JSONObject emptyJson = new JSONObject();

    public JSONObject compare(JSONObject oriMessage, JSONObject destMessage) {
        return emptyJson;
    }

    @Override
    public MonitorItem occureError(Exception e, String... messages) {
        return this;
    }

    @Override
    public void addMessage(String key, JSONObject value) {

    }
}
