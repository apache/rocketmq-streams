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
package org.apache.rocketmq.streams.filter.engine.impl;

import java.util.ArrayList;
import java.util.List;
import javax.swing.plaf.basic.BasicOptionPaneUI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.monitor.TopologyFilterMonitor;
import org.apache.rocketmq.streams.common.monitor.group.MonitorCommander;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.filter.contants.RuleStatus;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.context.RuleMessage;
import org.apache.rocketmq.streams.filter.engine.IRuleEngine;
import org.apache.rocketmq.streams.filter.exception.RegexTimeoutException;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.Action;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;

public class DefaultRuleEngine implements IRuleEngine {

    private static final Log LOG = LogFactory.getLog(DefaultRuleEngine.class);

    private static final Log RULEENGINE_MESSAGE_LOG = LogFactory.getLog("ruleengine_message");

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Override
    public List<Rule> executeRule(IMessage message, List<Rule> rules) {
        return executeRule(null, message, rules, true);
    }

    @Override
    public List<Rule> executeRule(AbstractContext context, IMessage message, List<Rule> rules) {
        return executeRule(context, message, rules, true);
    }

    protected List<Rule> executeRule(AbstractContext context, IMessage message, List<Rule> rules, boolean isAction) {
        IMonitor rulesMonitor = null;
        IMonitor monitor = null;
        if (context != null) {
            monitor = context.getCurrentMonitorItem();
            monitor = context.startMonitor("rules");
            rulesMonitor = monitor;
        }
        List<Rule> fireRules = new ArrayList<>();
        List<Rule> excuteRules = rules;
        try {
            if (excuteRules == null) {
                return fireRules;
            }
            try {
                boolean isTrace= TraceUtil.hit(message.getHeader().getTraceId());
                for (Rule rule : excuteRules) {
                    RuleContext ruleContext = new RuleContext(message.getMessageBody(), rule);
                    if (context != null) {
                        context.syncSubContext(ruleContext);
                    }
                    IMonitor ruleMonitor = null;
                    if (monitor != null) {
                        ruleMonitor = monitor.createChildren(rule);
                        ruleContext.setRuleMonitor(ruleMonitor);
                    }

                    try {

                        boolean isFireRule = processExpress( rule,ruleContext,isTrace);
                        if (isFireRule == false&&isTrace) {
                            TopologyFilterMonitor piplineExecutorMonitor = message.getHeader().getPiplineExecutorMonitor();
                            if (piplineExecutorMonitor != null) {
                                piplineExecutorMonitor.setNotFireRule(rule.getConfigureName());
                                piplineExecutorMonitor.setNotFireExpression2DependentFields(ruleContext.getExpressionMonitor().getNotFireExpression2DependentFields());
                            }
                        }
                        if (ruleMonitor != null) {
                            ruleMonitor.addContextMessage(isFireRule);

                        }

                        if (isFireRule) {
                            fireRules.add(rule);

                            //doUnRepeateScript(ruleContext, rule);// 执行规则去重脚本
                            if (isAction) {
                                fireAction(ruleContext, rule);
                            }
                        }
                        if (ruleMonitor != null) {
                            ruleMonitor.endMonitor();
                            if (ruleMonitor.isSlow()) {
                                ruleMonitor.setSampleData(ruleContext).put("rule_info", rule.toJsonObject());
                            }
                        }

                    } catch (Exception e) {
                        if (!RegexTimeoutException.class.isInstance(e)) {
                            LOG.error("DefaultRuleEngine executeRule for excuteRules error: rule name is: "
                                + rule.getConfigureName(), e);
                        }
                        if (ruleMonitor != null) {
                            ruleMonitor.occureError(e, e.getMessage());
                            ruleContext.addErrorMessage(rule, e.getMessage());
                            ruleMonitor.setSampleData(ruleContext).put("rule_info", rule.toJsonObject());
                        }

                    }
                }
            } catch (Exception e) {
                LOG.error("DefaultRuleEngine executeRule error,excuteRules size is " + excuteRules.size()
                    + " ,fireRules size is :" + fireRules.size(), e);
            }
        } catch (Exception e) {
            LOG.error(
                "DefaultRuleEngine executeRule error: fireRules size is: " + fireRules.size() + "excuteRules size is : "
                    + excuteRules.size(), e);

        }
        if (rulesMonitor != null) {
            rulesMonitor.setType(IMonitor.TYPE_DATAPROCESS);
            rulesMonitor.endMonitor();
            MonitorCommander.getInstance().finishMonitor(rulesMonitor.getName(), rulesMonitor);
        }
        return fireRules;

    }

    @Override
    public List<Rule> executeRuleWithoutAction(IMessage message, List<Rule> rules) {
        return executeRule(null, message, rules, false);
    }

    @SuppressWarnings("rawtypes")
    private void fireAction(RuleContext context, Rule rule) {
        if (rule == null) {
            LOG.error("DefaultRuleEngine fireAction error: rules is null!");
            return;
        }
        try {
            /**
             * 判断rule的status 如果是观察者模式，则写入到观察者库中的观察表里
             */
            if (rule.getRuleStatus().intValue() == RuleStatus.RULE_STATUS_ONLINE.getRuleStatus().intValue()) {// 上线状态
                if (!context.getContextConfigure().isAction2Online()) {
                    // LOG.warn("DefaultRuleEngine fireAction ignore : configure action2Online false!");
                    if (context.getContextConfigure().isActionOnline2Observer()) {
                        Action action = context.getAction(RuleContext.OBSERVER_NAME);
                        if (action == null) {
                            return;
                        }
                        doAction(context, action, rule);
                    }
                    return;
                }
                try {
                    if (rule.getActionNames() == null || rule.getActionNames().size() == 0) {
                        return;
                    }
                    for (String actionName : rule.getActionNames()) {
                        Action action = context.getAction(actionName);
                        if (action == null) {
                            continue;
                        }
                        doAction(context, action, rule);
                    }
                } catch (Exception e) {
                    LOG.error("DefaultRuleEngine fire atciton error: ruleName is" + rule.getConfigureName(), e);
                    context.addErrorMessage(rule, "DefaultRuleEngine fire atciton error: " + e.getMessage());
                }
            } else {
                if (context.getContextConfigure() != null && !context.getContextConfigure().isAction2Observer()) {
                    return;
                }
                Action action = context.getAction(RuleContext.OBSERVER_NAME);
                if (action == null) {
                    return;
                }
                doAction(context, action, rule);
            }

        } catch (Exception e) {
            LOG.error("DefaultRuleEngine fireAction error: ruleName is" + rule.getConfigureName(), e);
        }

    }

    /**
     * 处理一个规则的一个action
     *
     * @param context
     * @param rule
     */
    @SuppressWarnings("rawtypes")
    protected void doAction(final RuleContext context, final Action action, final Rule rule) {
        context.getActionExecutor().execute(new Runnable() {

            @Override
            public void run() {
                IMonitor monitor = context.getRuleMonitor();
                IMonitor actionMonitor = monitor.createChildren(action);
                try {

                    action.doAction(context, rule);
                    if (monitor != null) {
                        actionMonitor.endMonitor();
                        if (actionMonitor.isSlow()) {
                            actionMonitor.setSampleData(context).put("action_info", action.toJsonObject());
                        }
                    }
                } catch (Exception e) {
                    String errorMsg = "DefaultRuleEngine doAction error,rule: " + rule.getRuleCode() + " ,action: "
                        + action.getConfigureName();
                    //                    RULEENGINE_MESSAGE_LOG.warn(errorMsg
                    //                        , e);
                    actionMonitor.occureError(e, errorMsg, e.getMessage());
                    actionMonitor.setSampleData(context).put("action_info", action.toJsonObject());
                }

            }
        });
    }


    /**
     * 处理Express
     *
     * @param rule
     * @param context
     * @return
     */
    @SuppressWarnings("rawtypes")
    private boolean processExpress(Rule rule, RuleContext context, boolean isTrace) {
        try {
            if (rule.getExpressionName() == null) {
                return false;
            }
            
            Expression expression = context.getExpression(rule.getExpressionName());
            if (expression == null) {
                return false;
            }

            boolean match = expression.getExpressionValue(context, rule);
            if (isTrace&&!RelationExpression.class.isInstance(expression)) {
                TopologyFilterMonitor piplineExecutorMonitor = new TopologyFilterMonitor();
                piplineExecutorMonitor.addNotFireExpression(expression.toString(), expression.getDependentFields(rule.getExpressionMap()));
                context.setExpressionMonitor(piplineExecutorMonitor);
            }
            if (!match) {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("DefaultRuleEngine processExpress error,rule is: " + rule.getConfigureName(), e);
            return false;
        }
        return true;
    }

}
