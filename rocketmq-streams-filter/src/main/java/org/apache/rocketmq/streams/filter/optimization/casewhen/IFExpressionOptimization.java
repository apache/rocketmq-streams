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
package org.apache.rocketmq.streams.filter.optimization.casewhen;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.FilterResultCache;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class IFExpressionOptimization implements IScriptOptimization.IOptimizationCompiler {

    protected List<IScriptExpression> oriScriptExpressions;
    protected List<IScriptExpression> optimizationScriptExpressions;

    public IFExpressionOptimization(String namespace, String name, List<IScriptExpression> scriptExpressions) {
        this.oriScriptExpressions = scriptExpressions;
        this.optimizationScriptExpressions = this.oriScriptExpressions;
        List<IScriptExpression> scriptExpressionList = CaseWhenBuilder.compile(namespace, name, scriptExpressions);
        List<IScriptExpression> optimizationScriptExpressions = new ArrayList<>();
        boolean startIfScript = false;
        boolean startOtherScript = false;
        boolean canOptimize = true;
        IFCaseWhenExpressionGroup ifCaseWhenExpressionGroup = new IFCaseWhenExpressionGroup(MapKeyUtil.createKey(namespace, name));
        for (IScriptExpression scriptExpression : scriptExpressionList) {
            if (IFCaseWhenExpression.class.isInstance(scriptExpression)) {
                if (startIfScript == false) {
                    startIfScript = true;
                    optimizationScriptExpressions.add(ifCaseWhenExpressionGroup);
                }
                if (startOtherScript) {
                    canOptimize = false;
                    break;
                }
                IFCaseWhenExpression ifCaseWhenExpression = (IFCaseWhenExpression) scriptExpression;
                int index = ifCaseWhenExpressionGroup.addIFCaseWhewnExpression(ifCaseWhenExpression);
                ifCaseWhenExpression.setIndex(index);
            } else {
                if (startIfScript && !startOtherScript == false) {
                    startOtherScript = true;
                }
                optimizationScriptExpressions.add(scriptExpression);

            }
        }
        if (!canOptimize) {
            this.optimizationScriptExpressions = oriScriptExpressions;
        }
        if (ifCaseWhenExpressionGroup.size() > 50) {
            this.optimizationScriptExpressions = optimizationScriptExpressions;
        }
    }

    @Override public FilterResultCache execute(IMessage message, AbstractContext context) {
        throw new RuntimeException("can not support this method");
    }

    @Override public List<IScriptExpression> getOptimizationExpressionList() {
        return optimizationScriptExpressions;
    }
}
