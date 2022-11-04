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
package org.apache.rocketmq.streams.common.topology.stages;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import javax.swing.ImageIcon;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.component.IComponent;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IFilterService;
import org.apache.rocketmq.streams.common.monitor.ConsoleMonitorManager;
import org.apache.rocketmq.streams.common.monitor.TopologyFilterMonitor;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.metric.NotFireReason;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public class FilterChainStage<T extends IMessage, R extends AbstractRule> extends AbstractStatelessChainStage<T> implements IAfterConfigurableRefreshListener {
    protected transient AtomicInteger count = new AtomicInteger(0);
    protected transient Map<String, Integer> map = new HashMap<>();
    private List<String> names;
    /**
     * 通过名字匹配模式，加载一批规则，避免大批量输入规则名称
     */
    protected String nameRegex;
    private transient List<R> rules;
    private transient Map<String, JSONObject> ruleName2JsonObject = new HashMap<>();
    public static transient Class componentClass = ReflectUtil.forClass("org.apache.rocketmq.streams.filter.FilterComponent");
    protected boolean openHyperscan = false;
    protected static transient IComponent<IFilterService> component;
    protected transient FilterChainStage<T, R> SELF;

    public FilterChainStage() {
        SELF = this;
        setEntityName("filter");

    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            boolean isTrace = TraceUtil.hit(message.getHeader().getTraceId());

            if (component == null) {
                component = ComponentCreator.getComponent(null, componentClass);
            }
            String fieldValue = message.getHeader().getLogFingerprintValue();
            NotFireReason notFireReason = null;
            if (preFingerprint != null) {
                notFireReason = new NotFireReason(SELF, fieldValue);
                context.setNotFireReason(notFireReason);
            }

            List<R> fireRules = component.getService().executeRule(message, context, rules);

            //not match rules
            if (fireRules == null || fireRules.size() == 0) {
                context.breakExecute();
                if (preFingerprint != null) {
                    preFingerprint.addLogFingerprintToSource(message);
                }
                notFireReason = context.getNotFireReason();
                if (isTrace && notFireReason != null) {
                    traceFailExpression(message, notFireReason);
                }
            }
            return message;
        }

        @Override
        public String getName() {
            return FilterChainStage.class.getName();
        }
    };

    protected void traceFailExpression(IMessage message, NotFireReason notFireReason) {
        ConsoleMonitorManager.getInstance().reportOutput(FilterChainStage.this, message, ConsoleMonitorManager.MSG_FILTERED, notFireReason.toString());
        TraceUtil.debug(message.getHeader().getTraceId(), "break rule", notFireReason.toString());
    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        if (names == null || names.size() == 0) {
            List<AbstractRule> ruleList = configurableService.queryConfigurableByType(AbstractRule.TYPE);
            if (ruleList != null && ruleList.size() > 0) {
                //                rules= (R[]) Array.newInstance(ruleList.get(0).getClass(), ruleList.size());
                List<AbstractRule> matchedRules = new ArrayList<>();
                for (int i = 0; i < ruleList.size(); i++) {
                    if (StringUtil.isNotEmpty(nameRegex)) {
                        if (!StringUtil.matchRegex(ruleList.get(i).getConfigureName(), nameRegex)) {
                            continue;
                        }
                    }
                    matchedRules.add(ruleList.get(i));
                    //rules[i]=(R)ruleList.get(i);
                    ruleName2JsonObject.put(ruleList.get(i).getConfigureName(), ruleList.get(i).toOutputJson());
                }
                rules = new ArrayList<>();
                for (int i = 0; i < rules.size(); i++) {
                    rules.add((R) matchedRules.get(i));
                }
            }
        } else {
            if (names != null && names.size() > 0) {
                rules = new ArrayList<>();
            }
            int i = 0;
            ChainPipeline pipline = (ChainPipeline) getPipeline();
            String filterName = getLabel();
            for (String name : names) {
                AbstractRule rule = configurableService.queryConfigurable(AbstractRule.TYPE, name);
                if (rule == null) {
                    throw new RuntimeException("the rule expect exist, but not. the rule name is " + name);
                }
                rules.add((R) rule);
                if (!this.isOpenHyperscan()) {
                    /**
                     * open hyperscan to optimaztion mutil regex
                     */
                    String key = MapKeyUtil.createKeyBySign(".", pipline.getNameSpace(), pipline.getConfigureName(), filterName, "open_hyperscan");
                    String openHyperscan = ComponentCreator.getProperties().getProperty(key);
                    if (openHyperscan != null && Boolean.valueOf(openHyperscan)) {
                        this.openHyperscan = true;
                    }
                }
                if (isOpenHyperscan()) {
                    rule.setSupportHyperscan(true);
                }
                ruleName2JsonObject.put(rules.get(i).getConfigureName(), rules.get(i).toOutputJson());
                i++;
            }
        }
        if (this.preFingerprint == null) {
            this.preFingerprint = loadLogFinger();
        }

    }

    public void setRule(AbstractRule... rules) {
        if (rules == null || rules.length == 0) {
            return;
        }
        this.rules = new ArrayList<>();
        if (names == null) {
            names = new ArrayList<>();
        }
        int i = 0;
        for (AbstractRule rule : rules) {
            this.rules.add((R) rules[i]);
            names.add(rules[i].getConfigureName());
            ruleName2JsonObject.put(rules[i].getConfigureName(), rules[i].toOutputJson());
            i++;
        }
        setNameSpace(rules[0].getNameSpace());

    }

    public List<String> getNames() {
        return names;
    }

    public void setNames(List<String> names) {
        this.names = names;
    }

    public String getNameRegex() {
        return nameRegex;
    }

    public void setNameRegex(String nameRegex) {
        this.nameRegex = nameRegex;
    }

    public List<R> getRules() {
        return rules;
    }

    public boolean isOpenHyperscan() {
        return openHyperscan;
    }

    public void setOpenHyperscan(boolean openHyperscan) {
        this.openHyperscan = openHyperscan;
    }

    @Override
    public void setPreFingerprint(PreFingerprint preFingerprint) {
        this.preFingerprint = preFingerprint;
    }
}
