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
package org.apache.rocketmq.streams.connectors.source.client;

import com.alibaba.fastjson.JSONObject;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个分片的pull消费
 */
public class SplitConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SplitConsumer.class);
    private final AbstractPullSource pullSource;
    private final ISplit<?, ?> split;
    private final ExecutorService executorService;
    private SplitStatus splitStatus;
    private volatile boolean isFinished = true;
    private Long mLastCheckTime = System.currentTimeMillis();
    private ISplitReader reader;

    public SplitConsumer(AbstractPullSource pullSource, ISplit<?, ?> split, ExecutorService executorService) {
        this.pullSource = pullSource;
        this.split = split;
        this.executorService = executorService;
        this.splitStatus = SplitStatus.INIT;
    }

    public void open() {
        try {
            LOGGER.info("[{}][{}] Split_Open_On({})", IdUtil.instanceId(), pullSource.getName(), split.getQueueId());
            ISplitReader reader = pullSource.createReader(split);
            reader.open();
            String offset = pullSource.loadSplitOffset(split);
            reader.seek(offset);
            LOGGER.info("[{}][{}] Split_Reset_To({}:{})", IdUtil.instanceId(), pullSource.getName(), split.getQueueId(), offset == null ? "" : offset);
            this.reader = reader;
            Set<String> splits = new HashSet<>();
            splits.add(split.getQueueId());
            this.pullSource.addNewSplit(splits);
            splitStatus = SplitStatus.CONSUME;
        } catch (Exception e) {
            String error = "[{}][{}] Split_Open_Error_On({})";
            LOGGER.error(error, IdUtil.instanceId(), pullSource.getName(), split.getQueueId());
            throw new RuntimeException(e);
        }

    }

    public void consume() {
        if (isFinished) {
            isFinished = false;
            this.executorService.submit(this::exePullTask);
        }
    }

    public void destroy() {
        try {
            LOGGER.info("[{}][{}] Split_Destroy_On({})", IdUtil.instanceId(), pullSource.getName(), split.getQueueId());
            reader.interrupt();
            splitStatus = SplitStatus.INTERRUPT;
            Set<String> splitIds = new HashSet<>();
            splitIds.add(reader.getSplit().getQueueId());
            pullSource.removeSplit(splitIds);
            saveOffset(reader);
            reader.finish();
            splitStatus = SplitStatus.CLOSED;
        } catch (Exception e) {
            String error = "[{}][{}] Split_Destroy_Error_On({})";
            LOGGER.error(error, IdUtil.instanceId(), pullSource.getName(), split.getQueueId());
            throw new RuntimeException(error, e);
        }

    }

    protected void exePullTask() {
        try {
            if (!reader.isInterrupt()) {
                if (splitStatus == SplitStatus.CONSUME && reader.next()) {
                    LOGGER.info("[{}][{}] Split_Pull_Message_On({})", IdUtil.instanceId(), pullSource.getName(), split.getQueueId());
                    Iterator<PullMessage<?>> pullMessageIterator = reader.getMessage();
                    if (pullMessageIterator != null) {
                        while (pullMessageIterator.hasNext()) {
                            PullMessage<?> pullMessage = pullMessageIterator.next();
                            String queueId = split.getQueueId();
                            String offset = pullMessage.getOffsetStr();
                            JSONObject msg = pullSource.createJson(pullMessage.getMessage());
                            Message message = pullSource.createMessage(msg, queueId, offset, false);
                            message.getHeader().setOffsetIsLong(pullMessage.getMessageOffset().isLongOfMainOffset());
                            pullSource.executeMessage(message);
                        }
                    }
                }
                saveOffset(reader);
                isFinished = true;
            }
        } catch (Exception e) {
            String error = "[{}][{}] Split_Pull_Message_Error_On({})";
            LOGGER.error(error, IdUtil.instanceId(), pullSource.getName(), split.getQueueId(), e);
            throw new RuntimeException(error, e);
        }
    }

    /**
     * save offset
     *
     * @param reader
     */
    private void saveOffset(ISplitReader reader) {
        long curTime = System.currentTimeMillis();
        if (curTime - mLastCheckTime > pullSource.getCheckpointTime()) {
            if (!pullSource.isTest()) {
                LOGGER.info("[{}][{}] Split_Save_The_Progress_On({}:{})", IdUtil.instanceId(), pullSource.getName(), split.getQueueId(), reader.getProgress());
            }
            LOGGER.info("[{}][{}] Source_Delayed_On_Queue({})_For({})", IdUtil.instanceId(), pullSource.getName(), split.getQueueId(), reader.getDelay());
            pullSource.sendCheckpoint(reader.getSplit().getQueueId(), new MessageOffset(reader.getProgress()));
            mLastCheckTime = curTime;
        } else {
            if (!pullSource.isTest()) {
                LOGGER.debug("[{}][{}] Split_Save_The_Progress_ON({}:{}-{})", IdUtil.instanceId(), pullSource.getName(), split.getQueueId(), curTime, mLastCheckTime);
            }
        }
    }

    public ISplit<?, ?> getSplit() {
        return split;
    }

    protected enum SplitStatus {
        INIT,
        CONSUME,
        INTERRUPT,
        CLOSED
    }
}
