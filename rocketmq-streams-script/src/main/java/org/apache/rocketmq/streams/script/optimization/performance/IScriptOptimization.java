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
package org.apache.rocketmq.streams.script.optimization.performance;

import java.util.List;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.FilterResultCache;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * optimizate script ,include compile and execute
 */
public interface IScriptOptimization {

    static String getParameterValue(IScriptParamter scriptParamter) {
        if (!ScriptParameter.class.isInstance(scriptParamter)) {
            return null;
        }
        ScriptParameter parameter = (ScriptParameter) scriptParamter;
        if (parameter.getRigthVarName() != null) {
            return null;
        }
        return FunctionUtils.getConstant(parameter.getLeftVarName());
    }

    /**
     * compile expression list to improve performance
     *
     * @param expressions    IScriptExpression list of FunctionScript
     * @param functionScript functionScript object
     * @return the executor to to improve performance
     */
    IOptimizationCompiler compile(List<IScriptExpression> expressions, IConfigurableIdentification functionScript);

    /**
     * the executor can execute expression and return result
     */
    interface IOptimizationExecutor {
        FilterResultCache execute(IMessage message, AbstractContext context);
    }

    /**
     * the executor can execute expression and return result
     */
    interface IOptimizationCompiler extends IOptimizationExecutor {

        /**
         * the IScriptExpression list after Optimization compile
         *
         * @return
         */
        List<IScriptExpression> getOptimizationExpressionList();
    }
}
