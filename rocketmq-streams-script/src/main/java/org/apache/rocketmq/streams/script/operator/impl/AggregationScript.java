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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.function.aggregation.AverageAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.ConcatAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.ConcatDistinctAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.CountAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.CountDistinctAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.DistinctAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.MaxAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.MinAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.SumAccumulator;
import org.apache.rocketmq.streams.script.service.IAccumulator;

/**
 * 主要在window中使用，做统计计算使用
 */
public class AggregationScript implements IStreamOperator<IMessage, List<IMessage>> {

    private static final Log LOG = LogFactory.getLog(AggregationScript.class);

    private static Map<String, Class> aggregationEngineMap = new ConcurrentHashMap<String, Class>() {{
        put("max", MaxAccumulator.class);
        put("min", MinAccumulator.class);
        put("count", CountAccumulator.class);
        put("distinct", DistinctAccumulator.class);
        put("sum", SumAccumulator.class);
        put("avg", AverageAccumulator.class);
        put("concat_distinct", ConcatDistinctAccumulator.class);
        put("concat_agg", ConcatAccumulator.class);
        put("count_distinct", CountDistinctAccumulator.class);
    }};

    private String columnName;

    /**
     * aggregation function name
     */
    private String functionName;

    /**
     * parameter name
     */
    private String[] parameterNames;

    /**
     * guide the actors
     */
    private IAccumulator director;

    protected Object accumulator;
    protected List accumulators;
    /**
     * the way to accumulate: single or multi
     */
    public static final String INNER_AGGREGATION_COMPUTE_KEY = "_inner_aggregation_single_";

    public static final String INNER_AGGREGATION_COMPUTE_SINGLE = "single";

    public static final String INNER_AGGREGATION_COMPUTE_MULTI = "multi";

    public static final String INNER_AGGREGATION_VALUE_KEY = "_inner_aggregation_result_";

    /**
     * inner field in message
     */
    public static final String _INNER_AGGREGATION_FUNCTION_VALUE_ = "_INNER_AGGREGATION_FUNCTION_VALUE_";

    private static ScriptComponent scriptComponent = ScriptComponent.getInstance();

    public AggregationScript(String columnName, String functionName, String[] parameterNames) {
        this.functionName = functionName;
        this.columnName = columnName;
        this.parameterNames = parameterNames;
    }

    private AggregationScript() {

    }

    @Override
    public AggregationScript clone() {
        AggregationScript theClone = new AggregationScript(columnName, functionName, parameterNames);
        theClone.director = this.director;
        return theClone;
    }

    //region setter and getter

    public String getFunctionName() {
        return functionName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setDirector(IAccumulator director) {
        this.director = director;
    }

    public IAccumulator getDirector() {
        return director;
    }

    public void setParameterNames(String[] parameterNames) {
        this.parameterNames = parameterNames;
    }
    //endregion

    @Override
    public List<IMessage> doMessage(IMessage message, AbstractContext context) {
        if (director == null) {
            director = getAggregationFunction(functionName);
        }
        if (director != null) {
            String introduction = (String)message.getMessageBody().getOrDefault(INNER_AGGREGATION_COMPUTE_KEY, "");
            boolean isSingleAccumulate = INNER_AGGREGATION_COMPUTE_SINGLE.equals(introduction);
            boolean isMultiAccumulate = INNER_AGGREGATION_COMPUTE_MULTI.equals(introduction);
            if (isSingleAccumulate && accumulator != null && parameterNames != null) {
                Object[] values = getValueFromMessage(parameterNames, message);
                synchronized (accumulator) {
                    director.accumulate(accumulator, values);
                }
            } else if (isMultiAccumulate && accumulators != null) {
                director.merge(accumulator, accumulators);
            }
            message.getMessageBody().remove(INNER_AGGREGATION_COMPUTE_KEY);
            message.getMessageBody().put(columnName, director.getValue(accumulator));
        }
        List<IMessage> messages = new ArrayList<>();
        messages.add(message);
        return messages;
    }

    private Object[] getValueFromMessage(String[] parameterNames, IMessage message) {
        Object[] parameterValues = new Object[parameterNames.length];
        for (int index = 0; index < parameterNames.length; index++) {
            if (isConstValue(parameterNames[index])) {
                parameterValues[index] = parameterNames[index];
            } else {
                parameterValues[index] = message.getMessageBody().getOrDefault(parameterNames[index],
                    parameterNames[index]);
            }
        }
        return parameterValues;
    }

    private boolean isConstValue(String parameter) {
        return parameter.startsWith("\"") || parameter.startsWith("'");
    }

    public static IAccumulator getAggregationFunction(String functionName) {
        if (StringUtil.isEmpty(functionName)) {
            return null;
        }
        try {
            IAccumulator accumulator = scriptComponent.getFunctionService().getInnerInteface(functionName);
            if (accumulator != null) {
                return accumulator;
            }
            return aggregationEngineMap.containsKey(functionName) ? (IAccumulator)aggregationEngineMap.get(functionName)
                .newInstance() : null;

        } catch (Exception e) {
            LOG.error("failed in getting aggregation function, " + functionName, e);
        }
        return null;
    }

    public Object getAccumulator() {
        return accumulator;
    }

    public void setAccumulator(Object accumulator) {
        this.accumulator = accumulator;
    }

    public List getAccumulators() {
        return accumulators;
    }

    public void setAccumulators(List accumulators) {
        this.accumulators = accumulators;
    }
}
