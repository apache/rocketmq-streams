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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.impl.string.ToLowerFunction;
import org.apache.rocketmq.streams.script.function.impl.string.TrimFunction;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class IFCaseWhenExpressionGroup implements IScriptExpression {
    protected Map<String, List<IFCaseWhenExpression>> varName2ReturnNameIndexs = new HashMap<>();
    protected FingerprintCache cache = FingerprintCache.getInstance();
    protected Set<String> dependentFields = new HashSet<>();
    protected List<IScriptExpression> beforeExpressions = new ArrayList<>();

    protected int size = 0;

    protected String namespace;

    public IFCaseWhenExpressionGroup(String namespace) {
        this.namespace = namespace;
    }

    @Override public Object executeExpression(IMessage message, FunctionContext context) {

        if (CollectionUtil.isNotEmpty(beforeExpressions)) {
            for (IScriptExpression expression : beforeExpressions) {
                expression.executeExpression(message, context);
            }
        }
        for (String fieldNames : varName2ReturnNameIndexs.keySet()) {
            if (StringUtil.isEmpty(fieldNames)) {
                continue;
            }
            String fieldValue = null;
            if (fieldNames.indexOf(MapKeyUtil.SIGN) == -1) {
                fieldValue = getMsgValue(message, fieldNames);

            } else {
                String[] varNames = MapKeyUtil.splitKey(MapKeyUtil.SIGN);
                List<String> values = new ArrayList<>();
                for (String varName : varNames) {
                    String value = getMsgValue(message, varName);
                    values.add(value);

                }
                fieldValue = MapKeyUtil.createKey(values);
            }
            fieldValue = fieldNames + ";" + fieldValue;
            BitSetCache.BitSet bitSet = cache.getLogFingerprint(namespace, fieldValue);
            List<IFCaseWhenExpression> iFCaseWhenExpressionIndexs = varName2ReturnNameIndexs.get(fieldNames);
            if (bitSet == null) {
                bitSet = new BitSetCache.BitSet(iFCaseWhenExpressionIndexs.size());
                int index = 0;
                for (IFCaseWhenExpression ifCaseWhenExpression : iFCaseWhenExpressionIndexs) {
                    ifCaseWhenExpression.executeBefore(message, context);
                    boolean isMatch = (Boolean) ifCaseWhenExpression.getGroupScriptExpression().getIfExpresssion().executeExpression(message, context);
                    if (ifCaseWhenExpression.isReverse) {
                        isMatch = !isMatch;
                    }
                    if (isMatch) {
                        bitSet.set(index);
                    }
                    message.getMessageBody().put(ifCaseWhenExpression.getAsName(), isMatch);
                    index++;
                }
                cache.addLogFingerprint(namespace, fieldValue, bitSet);
            } else {
                int index = 0;
                for (IFCaseWhenExpression ifCaseWhenExpression : iFCaseWhenExpressionIndexs) {
                    message.getMessageBody().put(ifCaseWhenExpression.getAsName(), bitSet.get(index));
                    index++;
                }
            }

        }
        return null;
    }

    protected String getMsgValue(IMessage message, String fieldName) {
        String value = message.getMessageBody().getString(fieldName);
        if (StringUtil.isNotEmpty(value)) {
            return value;
        }
        return "N/A";
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
        return new ArrayList<>(dependentFields);
    }

    @Override public Set<String> getNewFieldNames() {
        return null;
    }

    public int addIFCaseWhewnExpression(IFCaseWhenExpression ifCaseWhenExpression) {
        size++;
        Set<String> varNames = new HashSet<>(ifCaseWhenExpression.getDependentFields());
        dependentFields.addAll(varNames);
        List<String> varNamesOrder = new ArrayList<>(varNames);
        Collections.sort(varNamesOrder);
        String fieldNames = MapKeyUtil.createKey(varNamesOrder);
        List<IFCaseWhenExpression> ifCaseWhenExpressions = varName2ReturnNameIndexs.get(fieldNames);
        if (ifCaseWhenExpressions == null) {
            ifCaseWhenExpressions = new ArrayList<>();
            varName2ReturnNameIndexs.put(fieldNames, ifCaseWhenExpressions);
        }
        ifCaseWhenExpressions.add(ifCaseWhenExpression);
        extraBeforeExpressions(ifCaseWhenExpression);
        return ifCaseWhenExpressions.size() - 1;
    }

    private void extraBeforeExpressions(IFCaseWhenExpression ifCaseWhenExpression) {
        List<IScriptExpression> beforeExpressions = ifCaseWhenExpression.groupScriptExpression.getBeforeExpressions();
        if (CollectionUtil.isNotEmpty(beforeExpressions)) {
            List<IScriptExpression> beforeExpressionList = new ArrayList<>();
            for (IScriptExpression scriptExpression : beforeExpressions) {
                if (TrimFunction.isTrimFunction(scriptExpression.getFunctionName()) || ToLowerFunction.isLowFunction(scriptExpression.getFunctionName())) {
                    this.beforeExpressions.add(scriptExpression);
                } else {
                    beforeExpressionList.add(scriptExpression);
                }
            }
            ifCaseWhenExpression.getGroupScriptExpression().setBeforeExpressions(beforeExpressionList);
        }
    }

    public int size() {
        return size;
    }
}
