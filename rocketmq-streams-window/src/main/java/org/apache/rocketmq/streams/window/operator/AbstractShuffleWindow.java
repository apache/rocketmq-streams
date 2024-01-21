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
package org.apache.rocketmq.streams.window.operator;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.optimization.MessageTrace;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.SectionPipeline;
import org.apache.rocketmq.streams.common.topology.stages.ShuffleChainStage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.stage.ShuffleOutputChainStage;
import org.apache.rocketmq.streams.stage.ShuffleSourceChainStage;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.shuffle.ShuffleSink;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractShuffleWindow extends AbstractWindow implements IShuffleCallback {
    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractShuffleWindow.class);
    /**
     * 聚合后的数据，继续走规则引擎的规则
     */
    protected static MemorySource memorySource = new MemorySource();
    protected SectionPipeline fireReceiver;
    protected transient ShuffleSink shuffleSink;

    @Override protected void startWindow() {
        super.startWindow();
        storage = new WindowStorage();
        storage.setLocalStorageOnly(isLocalStorageOnly);
    }

    @Override
    public int fire(WindowInstance windowInstance) {
        try {
            Set<String> splitIds = new HashSet<>();
            splitIds.add(windowInstance.getSplitId());
            if (shuffleSink != null) {
                shuffleSink.flush(splitIds);
            }
            return fireWindowInstance(windowInstance, windowInstance.getSplitId());
        } catch (Exception e) {
            Long time = this.eventTimeManager.getMaxEventTime(windowInstance.getSplitId());
            String maxEventTime = null;
            if (time != null) {
                maxEventTime = DateUtil.longToString(time);
            }
            LOGGER.error("[{}][{}] Window_Fire_Error_On({})_Window(startTime{}-endTime{}-fireTime{}-maxEventTime{})_ErrorMsg({})", IdUtil.instanceId(), NameCreator.getFirstPrefix(getName(), IWindow.TYPE), this.getClass().getName(), windowInstance.getStartTime(), windowInstance.getEndTime(), windowInstance.getFireTime(), maxEventTime, e.getMessage(), e);
            throw new RuntimeException(e);
        }

    }

    @Override public void sendFireMessage(List<WindowValue> windowValueList, String queueId) {
        try {
            int count = 0;
            List<IMessage> msgs = new ArrayList<>();
            for (WindowValue windowValue : windowValueList) {
                JSONObject message = new JSONObject();

                if (windowValue.getcomputedResult() instanceof JSONObject) {
                    message = (JSONObject) windowValue.getcomputedResult();
                } else {
                    Iterator<Map.Entry<String, Object>> it = windowValue.iteratorComputedColumnResult();
                    while (it.hasNext()) {
                        Map.Entry<String, Object> entry = it.next();
                        message.put(entry.getKey(), entry.getValue());
                    }
                }

                if (StringUtil.isNotEmpty(havingExpression)) {
                    boolean isMatch = ExpressionBuilder.executeExecute("tmp", havingExpression, message);
                    if (!isMatch) {
                        continue;
                    }
                }
                Long fireTime = DateUtil.parseTime(windowValue.getFireTime()).getTime();
                long baseTime = 1577808000000L;//set base time from 2021-01-01 00:00:00
                int sameFireCount = 0;
                if (fireMode != 0) {
                    Long endTime = DateUtil.parseTime(windowValue.getEndTime()).getTime();
                    sameFireCount = (int) ((fireTime - endTime) / 1000) / sizeInterval * timeUnitAdjust;
                    if (sameFireCount >= 1) {
                        sameFireCount = 1;
                    }
                }
                //can keep offset in order
                Long offset = ((fireTime - baseTime) / 1000 * 10 + sameFireCount) * 100000000 + windowValue.getPartitionNum();
                message.put("window_start", windowValue.getStartTime());
                message.put("window_end", windowValue.getEndTime());
                Message newMessage = memorySource.createMessage(message, queueId, offset + "", false);
                newMessage.getHeader().setOffsetIsLong(true);
                if (count == windowValueList.size() - 1) {
                    newMessage.getHeader().setNeedFlush(true);
                }

                msgs.add(newMessage);
                MessageTrace.joinMessage(newMessage);//关联全局监控器
                this.getFireReceiver().doMessage(newMessage, new Context(newMessage));

                count++;
            }

            if (DebugWriter.getDebugWriter(this.getName()).isOpenDebug()) {
                DebugWriter.getDebugWriter(this.getName()).writeWindowFire(this, msgs, queueId);
            }
        } catch (Exception e) {
            if (windowValueList != null) {
                WindowValue windowValue = windowValueList.get(0);
                Long time = this.eventTimeManager.getMaxEventTime(windowValue.getPartition());
                String maxEventTime = null;
                if (time != null) {
                    maxEventTime = DateUtil.longToString(time);
                }
                LOGGER.error("[{}][{}] Window_SendMsg2NextStage_Error_On({})_Window(startTime{}-endTime{}-fireTime{}-maxEventTime{})_ErrorMsg({})", IdUtil.instanceId(), NameCreator.getFirstPrefix(getName(), IWindow.TYPE), this.getClass().getName(), windowValue.getStartTime(), windowValue.getEndTime(), windowValue.getFireTime(), maxEventTime, e.getMessage(), e);

            }
            throw new RuntimeException(e);
        }

    }

    @Override
    public AbstractChainStage createStageChain(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);//必须放在第一个，为了是可以自动生成windowname，后续很多名称都是基于window创建的

        ShuffleOutputChainStage shuffleOutputChainStage = new ShuffleOutputChainStage();
        shuffleOutputChainStage.setWindow(this);
        shuffleOutputChainStage.setNameSpace(getNameSpace());
        shuffleOutputChainStage.setLabel(NameCreatorContext.get().createName("shuffle_out"));
        shuffleOutputChainStage.setName(shuffleOutputChainStage.getLabel());

        ShuffleSourceChainStage shuffleSourceChainStage = new ShuffleSourceChainStage();
        shuffleSourceChainStage.setWindow(this);
        shuffleSourceChainStage.setLabel(NameCreatorContext.get().createName("shuffle_source"));
        shuffleSourceChainStage.setName(shuffleSourceChainStage.getLabel());

        ShuffleChainStage shuffleChainStage = new ShuffleChainStage();
        shuffleChainStage.setConsumeChainStage(shuffleSourceChainStage);
        shuffleChainStage.setOutputChainStage(shuffleOutputChainStage);

        return shuffleChainStage;
    }

    @Override public void addConfigurables(PipelineBuilder pipelineBuilder) {

    }

    public ShuffleSink getShuffleSink() {
        return shuffleSink;
    }

    public void setShuffleSink(ShuffleSink shuffleSink) {
        this.shuffleSink = shuffleSink;
    }

    /**
     * 触发window
     *
     * @param instance
     */
    protected abstract int fireWindowInstance(WindowInstance instance, String queueId);

    public abstract void clearCache(String queueId);

    public SectionPipeline getFireReceiver() {
        return fireReceiver;
    }

    @Override public void setFireReceiver(SectionPipeline fireReceiver) {
        this.fireReceiver = fireReceiver;
    }
}
