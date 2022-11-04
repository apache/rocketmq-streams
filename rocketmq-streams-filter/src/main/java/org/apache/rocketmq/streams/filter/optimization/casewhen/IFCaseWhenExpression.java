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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.optimization.dependency.ScriptDependent;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class IFCaseWhenExpression implements IScriptExpression {
    protected String asName;
    protected GroupScriptExpression groupScriptExpression;
    protected int index;
    protected BitSetCache.BitSet bitSet;
    protected boolean isReverse=false;

    public IFCaseWhenExpression(GroupScriptExpression groupScriptExpression){
        this.groupScriptExpression=groupScriptExpression;
        this.asName=groupScriptExpression.getAfterExpressions().get(0).getNewFieldNames().iterator().next();
        IScriptExpression elseExpression=groupScriptExpression.getElseExpressions().get(0);
        IScriptParamter scriptParamter=(IScriptParamter)elseExpression.getScriptParamters().get(0);
        if(scriptParamter.getScriptParameterStr().toLowerCase().equals("true")){
            isReverse=true;
        }
    }


    public void executeBefore(IMessage message, FunctionContext context){
        groupScriptExpression.executeBeforeExpression(message,context);
    }

    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        return groupScriptExpression.executeExpression(message,context);
    }

    @Override public List<IScriptParamter> getScriptParamters() {
        return groupScriptExpression.getScriptParamters();
    }

    @Override public String getFunctionName() {
        return groupScriptExpression.getFunctionName();
    }

    @Override public String getExpressionDescription() {
        return groupScriptExpression.getExpressionDescription();
    }

    @Override public Object getScriptParamter(IMessage message, FunctionContext context) {
        return groupScriptExpression.getScriptParamter(message,context);
    }

    @Override public String getScriptParameterStr() {
        return groupScriptExpression.getScriptParameterStr();
    }

    @Override public List<String> getDependentFields() {
        ScriptDependent scriptDependent=new ScriptDependent(groupScriptExpression.getBeforeExpressions());
        Set<String> fieldNames=groupScriptExpression.getIFDependentFields();
        Set<String> dependentFields=new HashSet<>();
        if(fieldNames!=null){
            for(String fieldName:fieldNames){
                dependentFields.addAll(scriptDependent.traceaField(fieldName,new AtomicBoolean(false),new ArrayList<>()));
            }
            return new ArrayList<>(dependentFields);
        }
        return groupScriptExpression.getDependentFields();
    }

    @Override public Set<String> getNewFieldNames() {
        return groupScriptExpression.getNewFieldNames();
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public BitSetCache.BitSet getBitSet() {
        return bitSet;
    }

    public void setBitSet(BitSetCache.BitSet bitSet) {
        this.bitSet = bitSet;
    }

    public String getAsName() {
        return asName;
    }

    public GroupScriptExpression getGroupScriptExpression() {
        return groupScriptExpression;
    }

    public boolean isReverse() {
        return isReverse;
    }
}
