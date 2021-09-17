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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class CaseScriptExpressionProxy extends AbstractScriptProxy {
    public CaseScriptExpressionProxy(IScriptExpression origExpression) {
        super(origExpression);
    }

    @Override public List<ICacheFilter> getCacheFilters() {
        List<ICacheFilter> result = new ArrayList<>();
        GroupScriptExpression groupScriptExpression = (GroupScriptExpression) this.origExpression;
        recursion(groupScriptExpression, result);
        return result;
    }

    /**
     * recursion else if GroupScriptExpression list
     *
     * @param groupScriptExpression
     * @param cacheFilters
     */
    protected void recursion(GroupScriptExpression groupScriptExpression, List<ICacheFilter> cacheFilters) {
        IScriptExpression scriptExpression = groupScriptExpression.getIfExpresssion();
        AbstractScriptProxy abstractExpressionProxy = ScriptProxyFactory.getInstance().create(scriptExpression);
        if (abstractExpressionProxy != null) {
            groupScriptExpression.setIfExpresssion(abstractExpressionProxy);
            cacheFilters.addAll(abstractExpressionProxy.getCacheFilters());
        }
        if (groupScriptExpression.getElseIfExpressions() != null) {
            for (GroupScriptExpression expression : groupScriptExpression.getElseIfExpressions()) {
                recursion(expression, cacheFilters);
            }
        }
    }

    @Override public boolean supportOptimization(IScriptExpression scriptExpression) {
        if (scriptExpression instanceof GroupScriptExpression) {
            return true;
        }
        return false;
    }

    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        return this.origExpression.executeExpression(message, context);
    }
}
