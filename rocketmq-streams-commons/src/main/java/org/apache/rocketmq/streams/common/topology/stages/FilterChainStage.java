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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.component.IComponent;
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IFilterService;
import org.apache.rocketmq.streams.common.monitor.TopologyFilterMonitor;
import org.apache.rocketmq.streams.common.optimization.SQLLogFingerprintFilter;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public class FilterChainStage<T extends IMessage, R extends AbstractRule> extends AbstractStatelessChainStage<T> implements IAfterConfiguableRefreshListerner {
    protected transient AtomicInteger count = new AtomicInteger(0);
    protected transient Map<String, Integer> map = new HashMap<>();
    private List<String> names;
    protected String nameRegex;//通过名字匹配模式，加载一批规则，避免大批量输入规则名称
    private transient R[] rules;
    private transient Map<String, JSONObject> ruleName2JsonObject = new HashMap<>();
    public static transient Class componentClass = ReflectUtil.forClass("org.apache.rocketmq.streams.filter.FilterComponent");

    protected transient AbstractStage sourceStage;

    public FilterChainStage() {
        setEntityName("filter");
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            StringBuffer builder = new StringBuffer();
            if (rules != null) {
                for (int i = 0; i < rules.length; i++) {
                    if (rules[i] != null) {
                        builder.append(rules[0].getExpressionName()).append(";");
                    }
                }
            }
            TraceUtil.debug(message.getHeader().getTraceId(), "FilterChainStage",
                builder.toString(), message.getMessageBody().toJSONString());
            IComponent<IFilterService<R>> component = ComponentCreator.getComponent(null, componentClass);
            // long start=System.currentTimeMillis();
            message.getHeader().setPiplineExecutorMonitor(new TopologyFilterMonitor());
            List<R> fireRules = component.getService().executeRule(message, context, rules);
            //System.out.println("规则花费时间是:"+(System.currentTimeMillis()-start));
            if (fireRules == null || fireRules.size() == 0) {
                addLogFingerprintFilter(message);
                TopologyFilterMonitor monitor = message.getHeader().getPiplineExecutorMonitor();
                if (monitor != null) {
                    if (monitor.getNotFireExpression2DependentFields() != null) {

                        Map<String, List<String>> notFireExpressions = monitor.getNotFireExpression2DependentFields();
                        Iterator<Entry<String, List<String>>> it = notFireExpressions.entrySet()
                            .iterator();
                        String description = "the View  " + getOwnerSqlNodeTableName() + " break ,has " + notFireExpressions.size() + " expression not fire:" + PrintUtil.LINE;
                        StringBuilder stringBuilder = new StringBuilder(description);
                        int index = 1;
                        while (it.hasNext()) {

                            Entry<String, List<String>> entry = it.next();
                            String expression = entry.getKey();
                            List<String> dependentFields = entry.getValue();
                            for (String dependentField : dependentFields) {
                                List<String> scripts = findScriptByStage(dependentField);
                                if (scripts != null) {
                                    for (String script : scripts) {
                                        stringBuilder.append(script + PrintUtil.LINE);
                                    }

                                }
                            }
                            stringBuilder.append("The " + index++ + " expression is " + PrintUtil.LINE + getExpressionDescription(expression, message) + PrintUtil.LINE);

                        }
                        TraceUtil.debug(message.getHeader().getTraceId(), "break rule", stringBuilder.toString());
                    }

                }
                context.breakExecute();
                return message;
            }

            JSONArray jsonArray = new JSONArray();
            for (AbstractRule rule : fireRules) {
                JSONObject jsonObject = ruleName2JsonObject.get(rule.getConfigureName());
                jsonArray.add(jsonObject);
            }
            JSONArray fireRuleArray = message.getMessageBody().getJSONArray(AbstractRule.FIRE_RULES);
            if (fireRuleArray == null) {
                fireRuleArray = jsonArray;
            } else {
                fireRuleArray.addAll(jsonArray);
            }
            message.getMessageBody().put(AbstractRule.FIRE_RULES, fireRuleArray);
            return message;
        }

        @Override
        public String getName() {
            return FilterChainStage.class.getName();
        }
    };

    /**
     * 增加日志指纹
     *
     * @param message
     */
    protected void addLogFingerprintFilter(IMessage message) {
        if (sourceStage != null) {
            sourceStage.addLogFingerprint(message);
        }
    }

    /**
     * 如果是表达式，把表达式的值也提取出来
     *
     * @param expression
     * @param message
     * @return
     */
    protected String getExpressionDescription(String expression, IMessage message) {
        if (expression.startsWith("(")) {
            int index = expression.indexOf(",");
            String varName = expression.substring(1, index);
            String value = message.getMessageBody().getString(varName);
            String result = expression + ", the " + varName + " is " + value;
            return result;
        }
        return expression;
    }

    protected List<String> findScriptByStage(String notFireBooleanVar) {
        if (notFireBooleanVar == null || !notFireBooleanVar.startsWith("__")) {
            return null;
        }
        ScriptChainStage stage = findScriptChainStage(this);
        return stage.getDependentScripts(notFireBooleanVar);
    }

    protected ScriptChainStage findScriptChainStage(AbstractStage stage) {
        ChainPipeline pipline = (ChainPipeline)stage.getPipeline();
        if (pipline.isTopology()) {
            List<String> lableNames = stage.getPrevStageLabels();
            if (lableNames != null) {
                for (String lableName : lableNames) {
                    Map<String, AbstractStage> stageMap = pipline.getStageMap();
                    AbstractStage prewStage = stageMap.get(lableName);
                    if (prewStage != null && ScriptChainStage.class.isInstance(prewStage)) {
                        return (ScriptChainStage)prewStage;
                    }
                    if (prewStage != null) {
                        return findScriptChainStage(prewStage);
                    }
                }
            }
            return null;
        } else {
            List<AbstractStage> stages = pipline.getStages();
            int i = 0;
            for (; i < stages.size(); i++) {
                if (stages.get(i).equals(stage)) {
                    break;
                }
            }
            for (; i >= 0; i--) {
                AbstractStage prewStage = stages.get(i);
                if (ScriptChainStage.class.isInstance(prewStage)) {
                    return (ScriptChainStage)prewStage;
                }
            }
            return null;
        }
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
                rules = (R[])Array.newInstance(ruleList.get(0).getClass(), matchedRules.size());
                for (int i = 0; i < rules.length; i++) {
                    rules[i] = (R)matchedRules.get(i);
                }
            }
        } else {
            if (names != null && names.size() > 0) {
                AbstractRule rule = configurableService.queryConfigurable(AbstractRule.TYPE, names.get(0));
                rules = (R[])Array.newInstance(rule.getClass(), names.size());

            }
            int i = 0;
            for (String name : names) {
                AbstractRule rule = configurableService.queryConfigurable(AbstractRule.TYPE, name);
                rules[i] = (R)rule;
                ruleName2JsonObject.put(rules[i].getConfigureName(), rules[i].toOutputJson());
                i++;
            }
        }

        loadLogFinger();

    }

    public void setRule(AbstractRule... rules) {
        if (rules == null || rules.length == 0) {
            return;
        }
        this.rules = (R[])Array.newInstance(rules[0].getClass(), rules.length);
        if (names == null) {
            names = new ArrayList<>();
        }
        int i = 0;
        for (AbstractRule rule : rules) {
            this.rules[i] = (R)rules[i];
            names.add(rules[i].getConfigureName());
            ruleName2JsonObject.put(rules[i].getConfigureName(), rules[i].toOutputJson());
            i++;
        }
        setNameSpace(rules[0].getNameSpace());

    }

    /**
     * 从配置文件加载日志指纹信息，如果存在做指纹优化
     */
    protected void loadLogFinger() {
        ChainPipeline pipline = (ChainPipeline)getPipeline();
        String filterName = getLabel();
        if (pipline.isTopology() == false) {
            List<AbstractStage> stages = pipline.getStages();
            int i = 0;
            for (AbstractStage stage : stages) {
                if (stage == this) {
                    break;
                }
                i++;
            }
            filterName = i + "";
        }
        String key = MapKeyUtil.createKeyBySign(".", pipline.getNameSpace(), pipline.getConfigureName(), filterName);
        String logFingerFieldNames = ComponentCreator.getProperties().getProperty(key);
        if (logFingerFieldNames == null) {
            return;
        }
        sourceStage = getSourceStage();
        int index = logFingerFieldNames.indexOf(";");
        if (index != -1) {
            String filterIndex = logFingerFieldNames.substring(0, index);
            logFingerFieldNames = logFingerFieldNames.substring(index + 1);
        }
        sourceStage.setLogFingerFieldNames(logFingerFieldNames);
        sourceStage.setLogFingerFilterStageName(key);
        sourceStage.setLogFingerprintFilter(SQLLogFingerprintFilter.getInstance());
    }

    /**
     * 发现最源头的stage
     *
     * @return
     */
    protected AbstractStage getSourceStage() {
        ChainPipeline pipline = (ChainPipeline)getPipeline();
        if (pipline.isTopology()) {
            Map<String, AbstractStage> stageMap = pipline.createStageMap();
            AbstractStage currentStage = this;
            List<String> prewLables = currentStage.getPrevStageLabels();
            while (prewLables != null && prewLables.size() > 0) {
                if (prewLables.size() > 1) {
                    return null;
                }
                String lable = prewLables.get(0);
                AbstractStage stage = (AbstractStage)stageMap.get(lable);
                if (stage != null) {
                    currentStage = stage;
                } else {
                    return currentStage;
                }
                prewLables = currentStage.getPrevStageLabels();
            }
            return currentStage;
        } else {
            return (AbstractStage)pipline.getStages().get(0);
        }
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

    public AbstractRule[] getRules() {
        return rules;
    }
}
