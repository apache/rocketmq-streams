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
package org.apache.rocketmq.streams.filter.service.impl;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.filter.builder.RuleBuilder;
import org.apache.rocketmq.streams.filter.context.ContextConfigure;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.engine.IRuleEngine;
import org.apache.rocketmq.streams.filter.engine.impl.DefaultRuleEngine;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.service.IRuleEngineService;

/**
 * 规则引擎实现
 */
public class RuleEngineServiceImpl implements IRuleEngineService, Serializable {

    private static final long serialVersionUID = 5932020315482204865L;
    protected IRuleEngine ruleEngine = new DefaultRuleEngine();

    private ContextConfigure contextConfigure = new ContextConfigure(null);

    // @PostConstruct
    public void initRuleContext(ContextConfigure contextConfigure) {
        this.contextConfigure = contextConfigure;
        RuleContext.initSuperRuleContext(contextConfigure);
    }

    @Override
    public List<Rule> excuteRule(JSONObject message, Rule... rules) {
        Message msg = new Message(message);
        return this.executeRule(msg, new Context(msg), rules);
    }

    @Override
    public List<Rule> executeRule(IMessage message, AbstractContext context, Rule... rules) {
        if (rules == null || rules.length == 0) {
            return null;
        }
        List<Rule> rulesList = new ArrayList<>();

        if (rules != null) {
            for (Rule rule : rules) {
                rulesList.add(rule);
            }
        }
        if (context == null) {
            return ruleEngine.executeRule(message, rulesList);
        } else {
            return ruleEngine.executeRule(context, message, rulesList);
        }
    }

    @Override
    public List<Rule> excuteRule(JSONObject message, List<Rule> rules) {
        List<Rule> fireRules = ruleEngine.executeRule(new Message(message), rules);
        return fireRules;
    }

    @Override public List<Rule> executeRule(IMessage message, AbstractContext context, List<Rule> rules) {
        return this.ruleEngine.executeRule(context, message, rules);
    }

    @Override
    public Rule createRule(String namespace, String ruleName, String expressionStr, String... msgMetaInfo) {
        RuleBuilder ruleCreator = new RuleBuilder(namespace, ruleName, expressionStr, msgMetaInfo);
        Rule rule = ruleCreator.generateRule(null);
        return rule;
    }

}
