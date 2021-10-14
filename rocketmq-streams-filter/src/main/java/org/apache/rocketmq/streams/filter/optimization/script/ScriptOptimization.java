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
package org.apache.rocketmq.streams.filter.optimization.script;

import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.optimization.casewhen.CaseWhenBuilder;
import org.apache.rocketmq.streams.filter.optimization.executor.GroupByVarExecutor;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

@AutoService(IScriptOptimization.class)
public class ScriptOptimization  implements IScriptOptimization{

    public static interface IExpressionExecutor<T>{
        boolean execute(IMessage message, AbstractContext context);
    }


    @Override public IOptimizationCompiler compile(List<IScriptExpression> expressions, IConfigurableIdentification configurableIdentification) {
        if(expressions!=null){
            GroupByVarExecutor groupByVarExecutor=new GroupByVarExecutor(configurableIdentification.getNameSpace(),configurableIdentification.getConfigureName(),expressions);

            expressions=groupByVarExecutor.compile();
            expressions=CaseWhenBuilder.compile(configurableIdentification.getNameSpace(),configurableIdentification.getConfigureName(),expressions);
            groupByVarExecutor.setScriptExpressions(expressions);
            return groupByVarExecutor;
        }
        return null;
    }
}
