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
package org.apache.rocketmq.streams.common.channel.source;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.split.CommonSplit;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.BatchMessageOffset;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;

/**
 * 用批处理实现数据流 比如通过sql，定时获取数据，这类非消息队列数据源，没有offset和queueId，系统会模拟实现 也会增加offset的存储，实现断点续传
 */
public abstract class AbstractSingleSplitSource extends AbstractSource {
    /**
     * 模拟offset生成，递增产生
     */
    protected transient AtomicLong offsetGenerator;

    /**
     * 最后一次提交的时间，用来判断是否需要checkpoint
     */
    protected transient long lastCommitTime;

    private transient BatchMessageOffset progress;//如果需要保存offset，通过这个对象保存

    public AbstractSingleSplitSource() {

    }

    @Override
    protected boolean initConfigurable() {
        offsetGenerator = new AtomicLong(System.currentTimeMillis());
        return super.initConfigurable();
    }

    public AbstractContext doReceiveMessage(JSONObject message) {
        return doReceiveMessage(message, false);
    }

    @Override
    public JSONObject createJson(Object message) {
        if (isJsonData && JSONObject.class.isInstance(message)) {
            return (JSONObject) message;
        }
        return super.createJson(message);

    }

    public AbstractContext doReceiveMessage(String message, boolean needFlush) {
        String queueId = getQueueId();
        String offset = this.offsetGenerator.incrementAndGet() + "";
        return doReceiveMessage(message, needFlush, queueId, offset);
    }

    public AbstractContext doReceiveMessage(JSONObject message, boolean needFlush) {
        String queueId = getQueueId();
        String offset = this.offsetGenerator.incrementAndGet() + "";
        return doReceiveMessage(message, needFlush, queueId, offset);
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        super.addConfigurables(pipelineBuilder);
        if (progress != null) {
            pipelineBuilder.addConfigurables(progress);
        }
    }

    /**
     * 提供单条消息的处理逻辑，默认不会加入checkpoint
     *
     * @param message
     * @return
     */
    @Override
    public AbstractContext doReceiveMessage(JSONObject message, boolean needSetCheckPoint, String queueId,
        String offset) {
        Message msg = createMessage(message, queueId, offset, needSetCheckPoint);
        return executeMessage(msg);
    }

    /**
     * 对于批量接入的消息，可以在消息中加入checkpoint，在这批消息执行完成后，flush所有的输出节点，确保消息至少被消费一次
     *
     * @param messages          这批消息会作为一个批次
     * @param needSetCheckPoint 是否在最后一条消息加入checkpoint标志
     * @return
     */
    public AbstractContext doReceiveMessage(List<JSONObject> messages, boolean needSetCheckPoint) {
        if (messages == null || messages.size() == 0) {
            return null;
        }

        AbstractContext context = null;
        int i = 0;
        for (JSONObject jsonObject : messages) {

            if (i == messages.size() - 1) {
                doReceiveMessage(jsonObject, needSetCheckPoint);
            } else {
                doReceiveMessage(jsonObject, false);
            }
            i++;
        }
        return context;
    }

    public String getQueueId() {
        return RuntimeUtil.getDipperInstanceId();
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        ISplit<?, ?> split = new CommonSplit("1");
        List<ISplit<?, ?>> splits = new ArrayList<>();
        splits.add(split);
        return splits;
    }

    public Long createOffset() {
        return offsetGenerator.incrementAndGet();
    }
}
