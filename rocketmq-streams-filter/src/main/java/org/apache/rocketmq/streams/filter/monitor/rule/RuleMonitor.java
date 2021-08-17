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
package org.apache.rocketmq.streams.filter.monitor.rule;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.filter.monitor.Monitor;
import org.apache.rocketmq.streams.filter.monitor.contants.MonitorType;

@SuppressWarnings("rawtypes")
public class RuleMonitor extends Monitor<Boolean> {

    private Map<String, List<Monitor>> groupByMonitorClassName = new HashMap<>();

    @Override
    public void addChildren(Monitor monitor) {
        super.addChildren(monitor);
        List<Monitor> monitors = groupByMonitorClassName.get(monitor.getClass().getName());
        if (monitors == null) {
            monitors = new ArrayList<>();
            groupByMonitorClassName.put(monitor.getClass().getName(), monitors);
        }
        monitors.add(monitor);
    }

    public List<Monitor> getExpressionMonitors() {
        return groupByMonitorClassName.get(ExpressionMonitor.class.getName());
    }

    public List<Monitor> getActionMonitors() {
        return groupByMonitorClassName.get(ActionMonitor.class.getName());
    }

    public List<Monitor> getVarMonitors() {
        return groupByMonitorClassName.get(VarMonitor.class.getName());
    }

    @Override
    public JSONObject toJson() {
        return super.toJson();
    }

    @Override
    public String getType() {
        return MonitorType.RULE.name();
    }
}
