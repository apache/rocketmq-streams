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

import java.util.Set;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.apache.rocketmq.streams.common.functions.ReduceFunction;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.udf.IReducer;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

/**
 * 做windown 相关操作 可以同时设置多个统计算子，如count，sum，avg 通过toDataSteam/reduce 返回DataSteam
 */
public class WindowStream {
    //window 对象
    protected AbstractWindow window;

    /**
     * 创建datastream时使用
     */
    protected PipelineBuilder pipelineBuilder;
    protected Set<PipelineBuilder> otherPipelineBuilders;
    protected ChainStage<?> currentChainStage;

    public WindowStream(AbstractWindow window, PipelineBuilder pipelineBuilder, Set<PipelineBuilder> pipelineBuilders,
        ChainStage<?> currentChainStage) {
        this.pipelineBuilder = pipelineBuilder;
        this.otherPipelineBuilders = pipelineBuilders;
        this.currentChainStage = currentChainStage;
        this.window = window;
    }

    /**
     * 做count算子
     *
     * @param asName count结果对应的名字，如 sql中count(1) as c 。asName=c
     * @return
     */
    public WindowStream count(String asName) {
        window.getSelectMap().put(asName, asName + "=count(" + asName + ")");
        return this;
    }

    public WindowStream emitBeforeFire(Time emitTime) {
        window.setEmitBeforeValue(emitTime.getLongValue());
        return this;
    }

    public WindowStream emitAfterFire(Time emitTime) {
        window.setEmitAfterValue(emitTime.getLongValue());
        return this;
    }

    /**
     * distinct算子
     *
     * @param fieldName
     * @param asName
     * @return
     */
    public WindowStream distinct(String fieldName, String asName) {
        window.getSelectMap().put(asName, asName + "=distinct(" + fieldName + ")");
        return this;
    }

    /**
     * count_distinct算子
     *
     * @param fieldName
     * @param asName
     * @return
     */
    public WindowStream count_distinct(String fieldName, String asName) {
        String distinctName = "__" + fieldName + "_distinct_" + asName + "__";
        String prefix = distinctName + "=distinct(" + fieldName + ")";
        String suffix = asName + "=count(" + distinctName + ")";
        window.getSelectMap().put(asName, prefix + ";" + suffix);
        return this;
    }

    public WindowStream count_distinct_2(String fieldName, String asName) {
        String distinctName = "__" + fieldName + "_distinct_" + asName + "__";
        String prefix = distinctName + "=distinct2(" + fieldName + ",HIT_WINDOW_INSTANCE_ID,SHUFFLE_KEY)";
        String suffix = asName + "=count(" + distinctName + ")";
        window.getSelectMap().put(asName, prefix + ";" + suffix);
        return this;
    }

    /**
     * count_distinct算子（数据量大，容忍较少错误率）
     *
     * @param fieldName
     * @param asName
     * @return
     */
    public WindowStream count_distinct_large(String fieldName, String asName) {
        window.getSelectMap().put(asName, asName + "=count_distinct(" + fieldName + ")");
        return this;
    }

    /**
     * 做min算子
     *
     * @param fieldName 算子需要操作的字段名
     * @return
     */
    public WindowStream min(String fieldName) {
        window.getSelectMap().put(fieldName, fieldName + "=min(" + fieldName + ")");
        return this;
    }

    /**
     * 做max算子
     *
     * @param fieldName 算子需要操作的字段名
     * @return
     */
    public WindowStream max(String fieldName) {
        window.getSelectMap().put(fieldName, fieldName + "=max(" + fieldName + ")");
        return this;
    }

    /**
     * 做avg算子
     *
     * @param fieldName 算子需要操作的字段名
     * @param asName    avg结果对应的名字，如 sql中avg(name) as c 。asName=c
     * @return
     */
    public WindowStream avg(String fieldName, String asName) {
        window.getSelectMap().put(asName, asName + "=avg(" + fieldName + ")");
        return this;
    }

    /**
     * 做sum算子
     *
     * @param fieldName 算子需要操作的字段名
     * @param asName    sum结果对应的名字，如 sql中sum(name) as c 。asName=c
     * @return
     */
    public WindowStream sum(String fieldName, String asName) {
        window.getSelectMap().put(asName, asName + "=sum(" + fieldName + ")");
        return this;
    }

    public WindowStream setTimeField(String timeField) {
        window.setTimeFieldName(timeField);
        return this;
    }

    public WindowStream setFireMode(int fireMode) {
        window.setFireMode(fireMode);
        return this;
    }

    public WindowStream setLocalStorageOnly(boolean isLocalStorageOnley) {
        window.setLocalStorageOnly(isLocalStorageOnley);
        return this;
    }

    public WindowStream setMaxMsgGap(Long maxMsgGapSecond) {
        window.setMsgMaxGapSecond(maxMsgGapSecond);
        return this;
    }

    /**
     * 以哪几个字段做分组，支持多个字段
     *
     * @param fieldNames
     * @return
     */
    public WindowStream groupBy(String... fieldNames) {
        window.setGroupByFieldName(MapKeyUtil.createKeyBySign(";", fieldNames));
        for (String fieldName : fieldNames) {
            window.getSelectMap().put(fieldName, fieldName);
        }

        return this;
    }

    /**
     * 以哪几个字段做分组，支持多个字段
     *
     * @param fireMode
     * @return
     */
    public WindowStream fireMode(int fireMode) {
        window.setFireMode(fireMode);

        return this;
    }

    /**
     * 以哪几个字段做分组，支持多个字段
     *
     * @param waterMarkSecond
     * @return
     */
    public WindowStream waterMark(int waterMarkSecond) {
        window.setWaterMarkMinute(waterMarkSecond);

        return this;
    }

    /**
     * 以哪几个字段做分组，支持多个字段
     *
     * @param fieldName
     * @return
     */
    public WindowStream timeField(String fieldName) {
        window.setTimeFieldName(fieldName);

        return this;
    }

    /**
     * 用户自定义reduce逻辑
     *
     * @param reduceFunction
     * @return
     */
    public <R, O> DataStream reduce(ReduceFunction<R, O> reduceFunction) {
        window.setReducer((IReducer) (accumulator, msg) -> {
            Object accumulatorValue = accumulator;
            Object msgValue = msg;
            if (msg instanceof UserDefinedMessage) {
                UserDefinedMessage userDefinedMessage = (UserDefinedMessage) msg;
                msgValue = userDefinedMessage.getMessageValue();
            }
            if (accumulator instanceof UserDefinedMessage) {
                UserDefinedMessage userDefinedMessage = (UserDefinedMessage) accumulator;
                accumulatorValue = userDefinedMessage.getMessageValue();
            }
            R result = reduceFunction.reduce((R) accumulatorValue, (O) msgValue);
            return new UserDefinedMessage(result);
        });
        return new DataStream(pipelineBuilder, otherPipelineBuilders, currentChainStage);
    }

    public DataStream toDataStream() {
        return new DataStream(pipelineBuilder, otherPipelineBuilders, currentChainStage);
    }
}
