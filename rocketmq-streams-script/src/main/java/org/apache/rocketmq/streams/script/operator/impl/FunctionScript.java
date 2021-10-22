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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IBaseStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.optimization.FilterResultCache;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.parser.imp.FunctionParser;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

/**
 * * 对外提供的脚本算子，通过输入脚本，来实现业务逻辑 * 脚本存储的成员变量是value字段
 */
public class FunctionScript extends AbstractScript<List<IMessage>, FunctionContext> implements IStreamOperator<IMessage, List<IMessage>>, IStageBuilder<ChainStage> {

    private static final Log LOG = LogFactory.getLog(FunctionScript.class);

    /**
     * 脚本解析的表达式列表
     */
    private transient List<IScriptExpression> scriptExpressions = new ArrayList<IScriptExpression>();
    //protected transient ScriptExpressionGroupsProxy scriptExpressionGroupsProxy;
    /**
     * 表达式，转化成streamoperator接口列表，可以在上层中使用
     */
    private transient List<IBaseStreamOperator<IMessage, IMessage, FunctionContext>> receivers = new ArrayList<>();


    protected transient IScriptOptimization.IOptimizationCompiler optimizationCompiler;
    public FunctionScript() {setType(AbstractScript.TYPE);}

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
        /**
         * 健壮性检查，对于不符合规范的字符做转型
         */
        value = value.replace("’", "'");
        value = value.replace("‘", "'");
        value = value.replace("’", "'");
        this.scriptExpressions = FunctionParser.getInstance().parse(value);
        IScriptOptimization scriptOptimization=null;
        // optimize case when
        ServiceLoaderComponent serviceLoaderComponent=ServiceLoaderComponent.getInstance(IScriptOptimization.class);
        List<IScriptOptimization> scriptOptimizations=serviceLoaderComponent.loadService();
        if(scriptOptimizations!=null&&scriptOptimizations.size()>0){
            scriptOptimization=scriptOptimizations.get(0);
        }
        if (this.scriptExpressions == null) {
            LOG.debug("empty function");
        } else {
            List<IScriptExpression> expressions=this.scriptExpressions;
            if(scriptOptimization!=null){
                this.optimizationCompiler=scriptOptimization.compile(this.scriptExpressions,this);
                expressions =this.optimizationCompiler.getOptimizationExpressionList();
            }

            //转化成istreamoperator 接口
            for (IScriptExpression scriptExpression : expressions) {
                receivers.add((message, context) -> {
                    scriptExpression.executeExpression(message, context);
                    return message;
                });
            }
        }
        return true;
    }

    @Override
    public List<IMessage> doMessage(IMessage message, AbstractContext context) {


        FunctionContext functionContext = new FunctionContext(message);
        if (context != null) {
            context.syncSubContext(functionContext);
        }
        if(this.optimizationCompiler!=null){
            FilterResultCache quickFilterResult= this.optimizationCompiler.execute(message,functionContext);
            context.setQuickFilterResult(quickFilterResult);
        }
        List<IMessage> result = AbstractContext.executeScript(message, functionContext, this.receivers);
        if (context != null) {
            context.syncContext(functionContext);
        }
        return result;
    }

    @Override
    public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
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
                    Iterator<String> it = newFieldNames.iterator();
                    while (it.hasNext()) {
                        String newFieldName = it.next();
                        List<String> list = newFieldName2DependentFields.get(newFieldName);
                        if (list == null) {
                            list = new ArrayList<>();
                            newFieldName2DependentFields.put(newFieldName, list);
                        }
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

    protected transient Map<String, IScriptExpression> name2ScriptExpressions = null;

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
                    Map<String, IScriptExpression> map = new HashMap<>();
                    for (IScriptExpression expression : this.scriptExpressions) {
                        if (ScriptExpression.class.isInstance(expression)) {
                            ScriptExpression scriptExpression = (ScriptExpression)expression;
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
}
