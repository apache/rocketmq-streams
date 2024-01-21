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
package org.apache.rocketmq.streams.script.operator.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IBaseStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.model.ThreadContext;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.parser.imp.FunctionParser;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * * 对外提供的脚本算子，通过输入脚本，来实现业务逻辑 * 脚本存储的成员变量是value字段
 */
public class FunctionScript extends AbstractScript<List<IMessage>, FunctionContext> implements IStreamOperator<IMessage, List<IMessage>>, IStageBuilder<AbstractChainStage<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FunctionScript.class);
    protected transient IScriptOptimization.IOptimizationCompiler optimizationCompiler;
    //protected transient ScriptExpressionGroupsProxy scriptExpressionGroupsProxy;
    protected transient Map<String, IScriptExpression<?>> name2ScriptExpressions = null;
    protected transient AtomicBoolean hasStart = new AtomicBoolean(false);
    /**
     * 脚本解析的表达式列表
     */
    private transient List<IScriptExpression> scriptExpressions = new ArrayList<>();
    /**
     * 表达式，转化成streamoperator接口列表，可以在上层中使用
     */
    private transient List<IBaseStreamOperator<IMessage, IMessage, FunctionContext<?>>> receivers = new ArrayList<>();

    public FunctionScript() {
        setType(AbstractScript.TYPE);
    }

    public FunctionScript(String value) {
        this();
        this.value = value;
    }

    /**
     * 完成脚本的解析，并把IScriptExpression转化成IStreamOperator
     */
    @Override
    protected boolean initConfigurable() {
        String value = this.value;
        /*
          健壮性检查，对于不符合规范的字符做转型
         */
        value = value.replace("’", "'");
        value = value.replace("‘", "'");
        value = value.replace("’", "'");
        this.scriptExpressions = FunctionParser.getInstance().parse(value);

        //转化成istreamoperator 接口
        for (IScriptExpression<?> scriptExpression : this.scriptExpressions) {
            receivers.add((message, context) -> {
                scriptExpression.executeExpression(message, context);
                return message;
            });
        }
        doScriptOptimization();
        return true;
    }

    @Override
    public List<IMessage> doMessage(IMessage message, AbstractContext context) {

        FunctionContext functionContext = new FunctionContext(message);
        if (context != null) {
            context.syncSubContext(functionContext);
        }

        List<IMessage> result = executeScript(message, functionContext, this.receivers);

        if (context != null) {
            context.syncContext(functionContext);
        }
        return result;
    }

    public Object executeScriptAsFunction(IMessage message, AbstractContext context) {

        FunctionContext functionContext = new FunctionContext(message);
        if (context != null) {
            context.syncSubContext(functionContext);
        }

        executeScript(message, functionContext, this.receivers);

        Object returnValue = functionContext.getReturnValue();
        if (context != null) {
            context.syncContext(functionContext);
        }
        return returnValue;
    }

    @Override
    public AbstractChainStage createStageChain(PipelineBuilder pipelineBuilder) {
        ScriptChainStage scriptChainStage = new ScriptChainStage();
        pipelineBuilder.addConfigurables(this);
        scriptChainStage.setScript(this);
        //主要是为了打印pipline为文档格式时使用
        scriptChainStage.setEntityName("operator");
        return scriptChainStage;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {

    }

    @Override
    public Map<String, List<String>> getDependentFields() {
        Map<String, List<String>> newFieldName2DependentFields = new HashMap<>();
        if (this.scriptExpressions != null) {
            for (IScriptExpression scriptExpression : this.scriptExpressions) {
                Set<String> newFieldNames = scriptExpression.getNewFieldNames();
                if (newFieldNames != null && newFieldNames.size() > 0) {
                    List<String> fieldNames = scriptExpression.getDependentFields();
                    for (String newFieldName : newFieldNames) {
                        List<String> list = newFieldName2DependentFields.computeIfAbsent(newFieldName, k -> new ArrayList<>());
                        if (fieldNames != null) {
                            list.addAll(fieldNames);
                        }
                    }
                }
            }
        }
        return newFieldName2DependentFields;
    }

    /**
     * 脚本中用到的字段
     *
     * @return 字段数组
     */
    public String[] getDependentParameters() {
        Set<String> parameterSet = new HashSet<>();
        if (this.scriptExpressions != null) {
            for (IScriptExpression scriptExpression : this.scriptExpressions) {
                List<IScriptParamter> parameterList = scriptExpression.getScriptParamters();
                if (CollectionUtil.isEmpty(parameterList)) {
                    continue;
                }
                for (IScriptParamter scriptParameter : parameterList) {
                    parameterSet.add(scriptParameter.getScriptParameterStr());
                }
            }
        }
        return parameterSet.toArray(new String[0]);
    }

    /**
     * 跟定字段，查找对应的脚本
     *
     * @param fieldName 字段
     * @return 脚本列表
     */
    @Override
    public List<String> getScriptsByDependentField(String fieldName) {
        if (name2ScriptExpressions == null) {
            synchronized (this) {
                if (name2ScriptExpressions == null) {
                    Map<String, IScriptExpression<?>> map = new HashMap<>();
                    for (IScriptExpression<?> expression : this.scriptExpressions) {
                        if (expression instanceof ScriptExpression) {
                            ScriptExpression scriptExpression = (ScriptExpression) expression;
                            if (scriptExpression.getNewFieldName() != null) {
                                map.put(scriptExpression.getNewFieldName(), scriptExpression);
                            }
                        }

                    }
                    this.name2ScriptExpressions = map;
                }
            }
        }
        IScriptExpression expression = this.name2ScriptExpressions.get(fieldName);

        List<String> result = new ArrayList<>();
        findAllScript(expression, result);
        List<String> converse = new ArrayList<>();
        for (int i = result.size() - 1; i >= 0; i--) {
            converse.add(result.get(i));
        }
        return converse;
    }

    protected <R, C extends AbstractContext> List<IMessage> executeScript(IMessage channelMessage, C context,
        List<? extends IBaseStreamOperator<IMessage, R, C>> scriptExpressions) {
        List<IMessage> messages = new ArrayList<>();
        if (scriptExpressions == null) {
            return messages;
        }
        boolean isSplitMode = context.isSplitModel();
        context.closeSplitMode(channelMessage);
        int nextIndex = 1;
        //long start=System.currentTimeMillis();
        executeScript(scriptExpressions.get(0), channelMessage, context, nextIndex, scriptExpressions);

        if (!context.isContinue()) {
            context.setSplitModel(isSplitMode || context.isSplitModel());
            return null;
        }

        if (context.isSplitModel()) {
            messages = context.getSplitMessages();
        } else {
            messages.add(context.getMessage());
        }
        context.setSplitModel(isSplitMode || context.isSplitModel());
        return messages;
    }

    /**
     * 执行当前规则，如果规则符合拆分逻辑调拆分逻辑。为了是减少循环次数，一次循环多条规则
     *
     * @param currentExpression
     * @param channelMessage
     * @param context
     * @param nextIndex
     * @param scriptExpressions
     */
    protected <R, C extends AbstractContext> void executeScript(
        IBaseStreamOperator<IMessage, R, C> currentExpression,
        IMessage channelMessage, C context, int nextIndex,
        List<? extends IBaseStreamOperator<IMessage, R, C>> scriptExpressions) {
        //long start=System.currentTimeMillis();

        /**
         * 为了兼容blink udtf，通过localthread把context传给udtf的collector
         */
        ThreadContext threadContext = ThreadContext.getInstance();
        threadContext.set(context);
        currentExpression.doMessage(channelMessage, context);

        //System.out.println(currentExpression.toString()+" cost time is "+(System.currentTimeMillis()-start));
        if (context.isContinue() == false) {
            return;
        }
        if (nextIndex >= scriptExpressions.size()) {
            return;
        }
        IBaseStreamOperator<IMessage, R, C> nextScriptExpression = scriptExpressions.get(nextIndex);
        nextIndex++;
        if (context.isSplitModel()) {
            // start=System.currentTimeMillis();
            executeSplitScript(nextScriptExpression, channelMessage, context, nextIndex, scriptExpressions);

            //System.out.println(currentExpression.toString()+" cost time is "+(System.currentTimeMillis()-start));
        } else {
            executeScript(nextScriptExpression, channelMessage, context, nextIndex, scriptExpressions);
        }
    }

    protected <R, C extends AbstractContext> void executeSplitScript(
        IBaseStreamOperator<IMessage, R, C> currentExpression, IMessage channelMessage, C context, int nextIndex,
        List<? extends IBaseStreamOperator<IMessage, R, C>> scriptExpressions) {
        if (context.getSplitMessages() == null || context.getSplitMessages().size() == 0) {
            return;
        }
        List<IMessage> result = new ArrayList<>();
        List<IMessage> splitMessages = new ArrayList<IMessage>();
        splitMessages.addAll(context.getSplitMessages());
        int splitMessageOffset = 0;
        for (IMessage message : splitMessages) {
            context.closeSplitMode(message);
            message.getHeader().addLayerOffset(splitMessageOffset);
            splitMessageOffset++;
            executeScript(currentExpression, message, context, nextIndex, scriptExpressions);
            if (!context.isContinue()) {
                context.cancelBreak();
                continue;
            }
            if (context.isSplitModel()) {
                result.addAll(context.getSplitMessages());
            } else {
                result.add(context.getMessage());
            }

        }
        context.openSplitModel();
        context.setSplitMessages(result);
    }

    protected void findAllScript(IScriptExpression expression, List<String> result) {
        result.add(expression.toString());
        List<String> dependentFieldNames = expression.getDependentFields();
        if (dependentFieldNames == null || dependentFieldNames.size() == 0) {
            return;
        }
        for (String dependentFieldName : dependentFieldNames) {
            if (dependentFieldName.startsWith("__")) {
                IScriptExpression subExpression = this.name2ScriptExpressions.get(dependentFieldName);
                if (subExpression != null) {
                    findAllScript(subExpression, result);
                }
            }
        }
    }

    public List<IScriptExpression> getScriptExpressions() {
        return scriptExpressions;
    }

    public String getScript() {
        return value;
    }

    public void setScript(String script) {
        this.value = script;
    }

    protected void doScriptOptimization() {
        if (hasStart.compareAndSet(false, true)) {

            IScriptOptimization scriptOptimization = null;
            // optimize case when
            ServiceLoaderComponent serviceLoaderComponent = ServiceLoaderComponent.getInstance(IScriptOptimization.class);
            List<IScriptOptimization> scriptOptimizations = serviceLoaderComponent.loadService();
            if (scriptOptimizations != null && scriptOptimizations.size() > 0) {
                scriptOptimization = scriptOptimizations.get(0);
            }

            if (this.scriptExpressions == null) {
                LOGGER.debug("empty function");
            } else {
                List<IScriptExpression> expressions = this.scriptExpressions;
                if (scriptOptimization != null) {
                    this.optimizationCompiler = scriptOptimization.compile(this.scriptExpressions, this);
                    expressions = this.optimizationCompiler.getOptimizationExpressionList();
                }
                // this.scriptExpressions = expressions;
                List<IBaseStreamOperator<IMessage, IMessage, FunctionContext<?>>> newReceiver = new ArrayList<>();
                //转化成istreamoperator 接口
                for (IScriptExpression<?> scriptExpression : expressions) {
                    newReceiver.add((message, context) -> {
                        scriptExpression.executeExpression(message, context);
                        return message;
                    });
                }
                this.receivers = newReceiver;
            }
        }
    }
}
