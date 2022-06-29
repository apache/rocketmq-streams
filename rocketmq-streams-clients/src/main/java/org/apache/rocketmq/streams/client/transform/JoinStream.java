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

package org.apache.rocketmq.streams.client.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import org.apache.rocketmq.streams.window.operator.join.JoinWindow;

public class JoinStream {

    private static final String INNER_VAR_NAME_PREFIX = "___";
    protected JoinWindow joinWindow;//完成join 条件的添加
    protected String onCondition;//条件
    protected JoinType joinType;//连接类型

    //用于返回DataStream流
    protected PipelineBuilder pipelineBuilder;
    protected Set<PipelineBuilder> otherPipelineBuilders;
    protected ChainStage<?> currentChainStage;

    /**
     * 双流join 场景
     *
     * @param joinWindow
     * @param pipelineBuilder
     * @param pipelineBuilders
     * @param currentChainStage
     */
    public JoinStream(JoinWindow joinWindow, PipelineBuilder pipelineBuilder, Set<PipelineBuilder> pipelineBuilders,
        ChainStage<?> currentChainStage) {
        this.pipelineBuilder = pipelineBuilder;
        this.otherPipelineBuilders = pipelineBuilders;
        this.currentChainStage = currentChainStage;
        this.joinWindow = joinWindow;
    }

    public JoinStream(JoinWindow joinWindow, PipelineBuilder pipelineBuilder, Set<PipelineBuilder> pipelineBuilders,
        ChainStage<?> currentChainStage, JoinType joinType) {
        this.pipelineBuilder = pipelineBuilder;
        this.otherPipelineBuilders = pipelineBuilders;
        this.currentChainStage = currentChainStage;
        this.joinWindow = joinWindow;
        this.joinType = joinType;
    }

    /**
     * 设置join的类型， 推荐直接使用DataStream中的特定join来实现
     *
     * @param joinType
     * @return
     */
    @Deprecated
    public JoinStream setJoinType(JoinType joinType) {
        this.joinType = joinType;
        return this;
    }

    /**
     * 指定窗口，如果不指定，默认1个小时
     *
     * @param time
     * @return
     */
    public JoinStream window(Time time) {
        joinWindow.setTimeUnitAdjust(1);
        joinWindow.setSizeInterval(time.getValue());
        joinWindow.setSlideInterval(time.getValue());
        joinWindow.setRetainWindowCount(1);
        return this;
    }

    public JoinStream setLocalStorageOnly(boolean localStorageOnly) {
        this.joinWindow.setLocalStorageOnly(localStorageOnly);
        return this;
    }

    /**
     * 增加条件,用表达式形式表达(leftFieldName,function,rightFieldName)&&({name,==,otherName}||(age,==,age)) 后续再增加结构化的方法 。 后续该方法将下线，推荐使用on
     *
     * @param onCondition (leftFieldName,function,rightFieldName)&&({name,==,otherName}||(age,==,age))
     * @return
     */

    @Deprecated
    public JoinStream setCondition(String onCondition) {
        this.onCondition = onCondition;
        return this;
    }

    public JoinStream on(String onCondition) {
        this.onCondition = onCondition;
        return this;
    }

    public DataStream toDataStream() {
        return doJoin();
    }



    /**
     * 双流join的场景
     */
    protected DataStream doJoin() {
        if (JoinType.INNER_JOIN == joinType) {
            joinWindow.setJoinType("INNER");
        } else if (JoinType.LEFT_JOIN == joinType) {
            joinWindow.setJoinType("LEFT");
        } else {
            throw new RuntimeException("can not support this join type, expect INNER,LEFT, real is " + joinType.toString());
        }

        AtomicBoolean hasNoEqualsExpression = new AtomicBoolean(false);//是否有非等值的join 条件
        Map<String, String> left2Right = createJoinFieldsFromCondition(onCondition, hasNoEqualsExpression);//把等值条件的左右字段映射成map
        List<String> leftList = new ArrayList<>();
        List<String> rightList = new ArrayList<>();
        leftList.addAll(left2Right.keySet());
        rightList.addAll(left2Right.values());
        joinWindow.setLeftJoinFieldNames(leftList);
        joinWindow.setRightJoinFieldNames(rightList);
        //如果有非等值，则把这个条件设置进去
        if (hasNoEqualsExpression.get()) {
            joinWindow.setExpression(onCondition);
        }
        return new DataStream(pipelineBuilder, otherPipelineBuilders, currentChainStage);
    }

    /**
     * 支持的连接类型，目前支持inner join和left join
     */
    public enum JoinType {
        INNER_JOIN,
        LEFT_JOIN
    }

    /**
     * 从条件中找到join 左右的字段。如果有非等值，则不包含在内
     *
     * @param
     * @param onCondition
     * @return
     */
    public Map<String, String> createJoinFieldsFromCondition(String onCondition, AtomicBoolean hasNoEqualsExpression) {
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        ExpressionBuilder.createOptimizationExpression("tmp", "tmp", onCondition, expressions, relationExpressions);
        Map<String, String> left2Right = new HashMap<>();
        for (Expression<?> expression : expressions) {
            String varName = expression.getVarName();
            String valueName = expression.getValue().toString();
            if (!Equals.isEqualFunction(expression.getFunctionName())) {
                hasNoEqualsExpression.set(true);
                continue;
            }
            left2Right.put(varName, valueName);
        }
        return left2Right;
    }

    public static String createName(String functionName, String... names) {
        if (names == null || names.length == 0) {
            return NameCreator.createNewName(INNER_VAR_NAME_PREFIX, functionName);
        }
        String[] values = new String[names.length + 2];
        values[0] = INNER_VAR_NAME_PREFIX;
        values[1] = functionName;
        for (int i = 2; i < values.length; i++) {
            values[i] = names[i - 2];
        }
        return NameCreator.createNewName(values);
    }
}
