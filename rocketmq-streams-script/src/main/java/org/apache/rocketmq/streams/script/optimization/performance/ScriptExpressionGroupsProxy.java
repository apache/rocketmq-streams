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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterGroup;
import org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterManager;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class ScriptExpressionGroupsProxy extends CacheFilterManager implements IScriptExpression {
    protected List<IScriptExpression> scriptExpressions=new ArrayList<>();

    public ScriptExpressionGroupsProxy(int elementCount, int capacity) {
        super(elementCount, capacity);
    }
    public void removeLessCount() {
        Map<String, CacheFilterGroup> newFilterOptimizationMap=new HashMap<>();
        for(String varName:this.filterOptimizationMap.keySet()){
            CacheFilterGroup cacheFilterGroup =this.filterOptimizationMap.get(varName);
            if(cacheFilterGroup.getSize()>5){
                newFilterOptimizationMap.put(varName,cacheFilterGroup);
            }
        }
        this.filterOptimizationMap=newFilterOptimizationMap;
    }
    public void addScriptExpression(IScriptExpression scriptExpression){
        this.scriptExpressions.add(scriptExpression);
    }
    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        this.execute(message,context);
        for(IScriptExpression scriptExpression:scriptExpressions){
            scriptExpression.executeExpression(message,context);
        }
        return null;
    }

    @Override public List<IScriptParamter> getScriptParamters() {
        return null;
    }

    @Override public String getFunctionName() {
        return null;
    }

    @Override public String getExpressionDescription() {
        return null;
    }

    @Override public Object getScriptParamter(IMessage message, FunctionContext context) {
        return null;
    }

    @Override public String getScriptParameterStr() {
        return null;
    }

    @Override public List<String> getDependentFields() {
        return null;
    }

    @Override public Set<String> getNewFieldNames() {
        return null;
    }


}
