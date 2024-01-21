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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.filter.monitor.Monitor;
import org.apache.rocketmq.streams.filter.monitor.contants.MonitorType;
import org.apache.rocketmq.streams.filter.operator.Rule;

@SuppressWarnings("rawtypes")
public class MessageMonitor extends Monitor<List<Rule>> {
    public static final String KEY = MessageMonitor.class.getName();
    private static final NullMonitor NULL_MONITOR = new NullMonitor();
    private Map<String, Monitor> ruleMonitors = new HashMap<String, Monitor>();
    private volatile boolean monitorSwtich = true;

    public MessageMonitor(boolean monitorSwtich) {
        this.monitorSwtich = monitorSwtich;
    }

    public MessageMonitor() {
    }

    public Monitor createRuleMonitor(String ruleName) {
        if (!monitorSwtich) {
            return NULL_MONITOR;
        }
        RuleMonitor ruleMonitor = new RuleMonitor();
        ruleMonitor.begin(ruleName);
        this.addChildren(ruleMonitor);
        return ruleMonitor;
    }

    public Monitor createExpressionMonitor(String expressionName, String ruleName) {
        if (!monitorSwtich) {
            return NULL_MONITOR;
        }
        Monitor ruleMonitor = getRuleMonitor(ruleName);
        if (ruleMonitor == null) {
            return null;
        }
        ExpressionMonitor expressionMonitor = new ExpressionMonitor();
        expressionMonitor.begin(expressionName);
        ruleMonitor.addChildren(expressionMonitor);
        return expressionMonitor;
    }

    public Monitor createActionMonitor(String actionName, String ruleName) {
        return NULL_MONITOR;
    }

    @Override
    public void addChildren(Monitor monitor) {
        if (!monitorSwtich) {
            return;
        }
        super.addChildren(monitor);
        ruleMonitors.put(monitor.getMessage(), monitor);
    }

    public Monitor getRuleMonitor(String ruleName) {
        if (!monitorSwtich) {
            return NULL_MONITOR;
        }
        Monitor monitor = ruleMonitors.get(ruleName);
        if (monitor == null) {
            return null;
        }
        return (RuleMonitor) monitor;
    }

    @Override
    protected String resultToString() {
        if (result == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        boolean isStart = false;
        for (Rule rule : result) {
            if (isStart) {
                isStart = false;
            } else {
                sb.append(",");
            }
            sb.append(rule.getName());
        }
        return sb.toString();
    }

    @Override
    public String getType() {
        return MonitorType.MESSAGE.name();
    }

    public boolean isMonitorSwtich() {
        return monitorSwtich;
    }

    public void setMonitorSwtich(boolean monitorSwtich) {
        this.monitorSwtich = monitorSwtich;
    }
}
