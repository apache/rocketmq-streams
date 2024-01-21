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
package org.apache.rocketmq.streams.filter.context;

import java.util.Properties;

public class ContextConfigure {

    public static final int MONITOR_OPEN = 1;                  // 开启模式
    public static final int MONITOR_CLOSE = -1;                 // 关闭模式
    public static final int MONITOR_SAMPLE = 0;                  // 关闭模式
    public static final int MONITOR_STATISTICS = 2;                  // 只统计时间，不输出
    /**
     * 正则表达式优化相关
     */
    protected volatile boolean isSupportRegexOptimization = true;               // 是否支持正则表达式优化
    protected volatile int regexTimeout = 1000;               // 正则表达式超时时间，默认100ms
    protected int actionPoolSize = 5;
    /**
     * 控制监控输出
     */
    protected int monitorPoolSize = 10000;              // 监控对象池大小
    protected volatile boolean fetchDataInThisHost = false;              // 是否在本机采样，在采样模式下使用
    protected volatile int longTimeAlert = 1000;               // 规则超过500ms则打印输出
    protected volatile int mode = MONITOR_STATISTICS; // 0：开启模式；1，采样模式；-1：关闭模式
    /**
     * 区分引擎运行的环境
     */
    protected String evnNamespace;                                    // 区分压测环境和线上环境
    /**
     * 控制结果输出
     */
    protected boolean action2Online = true;               // 是否执行在线的action，如果是false，不执行线上的action
    protected boolean action2Observer = false;              // 观察中的规则是否输出到观察表，如果为true，则写到观察表
    protected boolean actionOnline2Observer = false;              // 在线规则输出到观察表，如果为true，则写到观察表，此种情况是为了测试
    /**
     * 控制加载规则
     */
    protected String loadRuleStatuses = "3";                // 规则加载方式，根据规则的状态加载
    protected String diyRuleCodes = null;               // 如果规则状态选择自定义，则此处列出规则的状态，用逗号分隔
    /**
     * 分流用对象
     */
    protected boolean openUnRepeateScript = false;
    //
    ///**
    // * 分流用对象
    // */
    //protected transient SplitFlow splitFlow = new SplitFlow();
    protected boolean openObserverDatasource = true;
    /**
     * 是否使用缓存处理正则
     */
    protected boolean encacheFlag = false;
    /**
     * 升级中心 压测数据
     */
    protected int pressureTotalCount = 10000;
    /**
     * 恶意IP最大加载数
     */
    protected int maliciousIPMaxCount = 1000000;
    private volatile String rulesLogsIps = "127.0.0.1";

    public ContextConfigure(Properties properties) {

    }

    /**
     * 是否支持监控模块输出超时日志
     *
     * @return
     */
    public boolean canMonitor() {
        if (mode == ContextConfigure.MONITOR_OPEN) {
            return true;
        }
        if (mode == ContextConfigure.MONITOR_SAMPLE && isFetchDataInThisHost()) {
            return true;
        }
        return false;
    }

    /**
     * 是否支持规则统计（统计慢规则）
     *
     * @return
     */
    public boolean canStatisticsMonitor() {
        if (mode == ContextConfigure.MONITOR_STATISTICS) {
            return true;
        }
        if (this.canMonitor()) {
            return true;
        }
        return false;
    }

    public boolean isSupportRegexOptimization() {
        return isSupportRegexOptimization;
    }

    public void setSupportRegexOptimization(boolean supportRegexOptimization) {
        isSupportRegexOptimization = supportRegexOptimization;
    }

    public int getRegexTimeout() {
        return regexTimeout;
    }

    public void setRegexTimeout(int regexTimeout) {
        this.regexTimeout = regexTimeout;
    }

    public boolean isFetchDataInThisHost() {
        return fetchDataInThisHost;
    }

    public void setFetchDataInThisHost(boolean fetchDataInThisHost) {
        this.fetchDataInThisHost = fetchDataInThisHost;
    }

    public int getLongTimeAlert() {
        return longTimeAlert;
    }

    public void setLongTimeAlert(int longTimeAlert) {
        this.longTimeAlert = longTimeAlert;
    }

    public String getEvnNamespace() {
        return evnNamespace;
    }

    public void setEvnNamespace(String evnNamespace) {
        this.evnNamespace = evnNamespace;
    }

    public boolean isAction2Online() {
        return action2Online;
    }

    public void setAction2Online(boolean action2Online) {
        this.action2Online = action2Online;
    }

    public boolean isAction2Observer() {
        return action2Observer;
    }

    public void setAction2Observer(boolean action2Observer) {
        this.action2Observer = action2Observer;
    }

    public boolean isActionOnline2Observer() {
        return actionOnline2Observer;
    }

    public void setActionOnline2Observer(boolean actionOnline2Observer) {
        this.actionOnline2Observer = actionOnline2Observer;
    }

    public String getLoadRuleStatuses() {
        return loadRuleStatuses;
    }

    public void setLoadRuleStatuses(String loadRuleStatuses) {
        this.loadRuleStatuses = loadRuleStatuses;
    }

    public String getDiyRuleCodes() {
        return diyRuleCodes;
    }

    public void setDiyRuleCodes(String diyRuleCodes) {
        this.diyRuleCodes = diyRuleCodes;
    }

    public String getRulesLogsIps() {
        return rulesLogsIps;
    }

    public void setRulesLogsIps(String rulesLogsIps) {
        this.rulesLogsIps = rulesLogsIps;
    }

    public int getMonitorPoolSize() {
        return monitorPoolSize;
    }

    public void setMonitorPoolSize(int monitorPoolSize) {
        this.monitorPoolSize = monitorPoolSize;
    }

    public int getMode() {
        return mode;
    }

    public void setMode(int mode) {
        this.mode = mode;
    }

    //public SplitFlow getSplitFlow() {
    //    return splitFlow;
    //}
    //
    //public void setSplitFlow(SplitFlow splitFlow) {
    //    this.splitFlow = splitFlow;
    //}

    public boolean isOpenUnRepeateScript() {
        return openUnRepeateScript;
    }

    public void setOpenUnRepeateScript(boolean openUnRepeateScript) {
        this.openUnRepeateScript = openUnRepeateScript;
    }

    public boolean isOpenObserverDatasource() {
        return openObserverDatasource;
    }

    public void setOpenObserverDatasource(boolean openObserverDatasource) {
        this.openObserverDatasource = openObserverDatasource;
    }

    public boolean isEncacheFlag() {
        return encacheFlag;
    }

    public void setEncacheFlag(boolean encacheFlag) {
        this.encacheFlag = encacheFlag;
    }

    public int getPressureTotalCount() {
        return pressureTotalCount;
    }

    public void setPressureTotalCount(int pressureTotalCount) {
        this.pressureTotalCount = pressureTotalCount;
    }

    public int getActionPoolSize() {
        return actionPoolSize;
    }

    public void setActionPoolSize(int actionPoolSize) {
        this.actionPoolSize = actionPoolSize;
    }
}
