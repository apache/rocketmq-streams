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

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.functions.ReduceFunction;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.udf.IReducer;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.InstantiationUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.script.operator.impl.AggregationScript;
import org.apache.rocketmq.streams.script.service.IAccumulator;
import org.apache.rocketmq.streams.script.service.udf.SimpleUDAFScript;
import org.apache.rocketmq.streams.script.service.udf.UDAFScript;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

/**
 * ???windown ???????????? ??????????????????????????????????????????count???sum???avg ??????toDataSteam/reduce ??????DataSteam
 */
public class WindowStream {
    //window ??????
    protected AbstractWindow window;

    /**
     * ??????datastream?????????
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
     * ???count??????
     *
     * @param asName count??????????????????????????? sql???count(1) as c ???asName=c
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
     * distinct??????
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
     * count_distinct??????
     *
     * @param fieldName
     * @param asName
     * @return
     */
    public WindowStream count_distinct(String fieldName, String asName) {
        return count_distinct_2(fieldName,asName);
    }

    public WindowStream addUDAF(IAccumulator accumulator, String asName,String... fieldNames) {
        AggregationScript.registUDAF(accumulator.getClass().getSimpleName(),accumulator.getClass());
        String prefix = asName + "="+accumulator.getClass().getSimpleName()+"(" + MapKeyUtil.createKeyBySign(",",fieldNames)+")";
        window.getSelectMap().put(asName,prefix);
        return this;
    }

    public WindowStream count_distinct_2(String fieldName, String asName) {
        String distinctName = "__" + fieldName + "_distinct_" + asName + "__";
        String prefix = distinctName + "=distinct2(" + fieldName + ",HIT_WINDOW_INSTANCE_ID,SHUFFLE_KEY)";
        String suffix = asName + "=count(" + distinctName + ")";
        window.getSelectMap().put(asName, prefix + ";" + suffix);
        return this;
    }

    public WindowStream saveWindowMsg(MapFunction<JSONObject, Pair<WindowInstance, JSONObject>> mapFunction ,String sinkType, Properties properties) {
        ServiceLoaderComponent<?> serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        IChannelBuilder builder = (IChannelBuilder) serviceLoaderComponent.loadService(sinkType.toLowerCase());

        if (builder == null) {
            throw new RuntimeException(
                "expect channel creator for " + sinkType+ ". but not found");
        }
        ISink<?> sink= builder.createSink(pipelineBuilder.getPipelineNameSpace(), window.getConfigureName(), properties, null);
        this.pipelineBuilder.addConfigurables(sink);
        this.window.setContextMsgSinkName(sink.getConfigureName());
        byte[] bytes=InstantiationUtil.serializeObject(mapFunction);
        String mapFunctionStr = Base64Utils.encode(bytes);
        this.window.setMapFunctionSerializeValue(mapFunctionStr);
        return this;
    }

    /**
     * count_distinct????????????????????????????????????????????????
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
     * ???min??????
     *
     * @param fieldName ??????????????????????????????
     * @return
     */
    public WindowStream min(String fieldName) {
        window.getSelectMap().put(fieldName, fieldName + "=min(" + fieldName + ")");
        return this;
    }

    /**
     * ???max??????
     *
     * @param fieldName ??????????????????????????????
     * @return
     */
    public WindowStream max(String fieldName) {
        window.getSelectMap().put(fieldName, fieldName + "=max(" + fieldName + ")");
        return this;
    }

    /**
     * ???avg??????
     *
     * @param fieldName ??????????????????????????????
     * @param asName    avg??????????????????????????? sql???avg(name) as c ???asName=c
     * @return
     */
    public WindowStream avg(String fieldName, String asName) {
        window.getSelectMap().put(asName, asName + "=avg(" + fieldName + ")");
        return this;
    }

    /**
     * ???sum??????
     *
     * @param fieldName ??????????????????????????????
     * @param asName    sum??????????????????????????? sql???sum(name) as c ???asName=c
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
     * ????????????????????????????????????????????????
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
     * ????????????????????????????????????????????????
     *
     * @param fireMode
     * @return
     */
    public WindowStream fireMode(int fireMode) {
        window.setFireMode(fireMode);

        return this;
    }

    /**
     * ????????????????????????????????????????????????
     *
     * @param waterMarkSecond
     * @return
     */
    public WindowStream waterMark(int waterMarkSecond) {
        window.setWaterMarkMinute(waterMarkSecond);

        return this;
    }

    /**
     * ????????????????????????????????????????????????
     *
     * @param fieldName
     * @return
     */
    public WindowStream timeField(String fieldName) {
        window.setTimeFieldName(fieldName);

        return this;
    }

    /**
     * ???????????????reduce??????
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
