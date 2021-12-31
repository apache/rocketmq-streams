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
package org.apache.rocketmq.streams.filter;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.component.IgnoreNameSpace;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.builder.RuleBuilder;
import org.apache.rocketmq.streams.filter.context.ContextConfigure;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.service.IRuleEngineService;
import org.apache.rocketmq.streams.filter.service.impl.RuleEngineServiceImpl;

/**
 * 通过表达式，做过滤 表达式的表达形式(varName,functionName,value)&varName,functionName,datatypeName,value)
 */
public class FilterComponent extends AbstractComponent<IRuleEngineService> implements IRuleEngineService, IgnoreNameSpace {

    private static final Log LOG = LogFactory.getLog(FilterComponent.class);

    private static final FilterComponent filterComponent = ComponentCreator.getComponent(null, FilterComponent.class);

    protected RuleEngineServiceImpl ruleEngineService;

    public FilterComponent() {
    }

    public static FilterComponent getInstance() {
        return filterComponent;
    }

    @Override
    public boolean stop() {
        return true;
    }

    @Override
    public IRuleEngineService getService() {
        return this.ruleEngineService;
    }

    @Override
    protected boolean startComponent(String namespace) {
        return true;
    }

    public static RuleEngineServiceImpl createRuleEngineService(Properties properties) {
        RuleEngineServiceImpl ruleEngineService = new RuleEngineServiceImpl();
        ContextConfigure config = new ContextConfigure(properties);
        ruleEngineService.initRuleContext(config);
        return ruleEngineService;
    }

    @Override
    protected boolean initProperties(Properties properties) {
        if (ruleEngineService != null) {
            return true;
        }
        RuleEngineServiceImpl ruleEngineService = createRuleEngineService(properties);

        this.ruleEngineService = ruleEngineService;
        return true;
    }

    @Override
    public Rule createRule(String namespace, String ruleName, String expressionStr, String... msgMetaInfo) {
        RuleBuilder ruleCreator = new RuleBuilder(namespace, ruleName, expressionStr, msgMetaInfo);
        Rule rule = ruleCreator.generateRule(null);
        return rule;
    }

    @Override
    public List<Rule> excuteRule(JSONObject message, Rule... rules) {
        return ruleEngineService.excuteRule(message, rules);
    }

    @Override
    public List<Rule> executeRule(IMessage message, AbstractContext context, Rule... rules) {
        return ruleEngineService.executeRule(message, context, rules);
    }

    @Override public List<Rule> executeRule(IMessage message, AbstractContext context, List<Rule> rules) {
        return ruleEngineService.executeRule(message, context, rules);
    }

    @Override
    public List<Rule> excuteRule(JSONObject message, List<Rule> rules) {
        return ruleEngineService.excuteRule(message, rules);
    }
}
