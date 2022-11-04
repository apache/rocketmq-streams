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
package org.apache.rocketmq.streams.connectors.source;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.AbstractCheckPointStorage;
import org.apache.rocketmq.streams.common.checkpoint.ICheckPointStorage;
import org.apache.rocketmq.streams.common.checkpoint.ISplitOffset;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.reader.SplitCloseFuture;
import org.apache.rocketmq.streams.dispatcher.ICache;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.impl.RocketmqDispatcher;
import org.apache.rocketmq.streams.tasks.cache.DBCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPullSource extends AbstractSource implements IPullSource<AbstractSource> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPullSource.class);

    protected transient Map<String, ISplitReader> ownerSplitReaders = new HashMap<>();//owner split readers
    protected transient Map<String, ISplit<?, ?>> ownerSplits = new HashMap<>();//working splits by the source instance

    //可以有多种实现，通过名字选择不同的实现
    protected long pullIntervalMs = 1000 * 20;
    protected transient volatile boolean shutDown = false;

    /**
     * object for balance
     */
    private transient RocketmqDispatcher<?> dispatcher;
    private transient IDispatcherCallback balanceCallback;

    private transient ICache cache;
    protected boolean isTest = false;
    protected boolean closeBalance = true;

    @Override protected boolean startSource() {
        if (this.cache == null) {
            this.cache = new DBCache();
        }
        if (isTest || closeBalance) {
            doSplitAddition(fetchAllSplits());
        }
        if (!isTest) {
            setOffsetStore();
        }

        if (!closeBalance) {
            startBalance();
        }
        startWorks();
        return true;
    }

    protected void setOffsetStore() {
        ICheckPointStorage iCheckPointStorage = new AbstractCheckPointStorage() {

            @Override public String getStorageName() {
                return "rocketmq";
            }

            @Override public <T extends ISplitOffset> void save(List<T> checkPointState) {
                if (checkPointState == null) {
                    return;
                }
                for (T splitOffset : checkPointState) {
                    String namespace = MapKeyUtil.createKey(getNameSpace(), getConfigureName());
                    String queueId = splitOffset.getQueueId();
                    String offset = splitOffset.getOffset();
                    cache.putKeyConfig(namespace, queueId, offset);
                }

            }

            @Override public ISplitOffset recover(ISource iSource, String queueID) {
                String name = MapKeyUtil.createKey(iSource.getNameSpace(), iSource.getConfigureName());
                String offset = cache.getKeyConfig(name, queueID);
                return new ISplitOffset() {

                    @Override public String getName() {
                        return name;
                    }

                    @Override public String getQueueId() {
                        return queueID;
                    }

                    @Override public String getOffset() {
                        return offset;
                    }
                };
            }
        };
        checkPointManager.setiCheckPointStorage(iCheckPointStorage);
    }

    protected void startBalance() {
        try {
            if (this.balanceCallback == null) {
                this.balanceCallback = new IDispatcherCallback() {

                    @Override public List<String> start(List<String> names) {
                        List<ISplit<?, ?>> additionSplits = getSplitByName(names);
                        if (CollectionUtil.isEmpty(additionSplits)) {
                            return names;
                        }
                        doSplitAddition(additionSplits);
                        return names;
                    }

                    @Override public List<String> stop(List<String> names) {
                        List<ISplit<?, ?>> additionSplits = getSplitByName(names);
                        if (CollectionUtil.isEmpty(additionSplits)) {
                            return names;
                        }
                        doSplitRelease(additionSplits);
                        return names;
                    }

                    @Override public List<String> list(List<String> instanceIdList) {
                        return new ArrayList<>(getAllSplitMap().keySet());
                    }
                };
            }
            if (this.dispatcher == null) {
                String dispatcherType = ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_TYPE, "rocketmq");
                if (dispatcherType.equalsIgnoreCase("rocketmq")) {
                    String nameServAddr = ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_ROCKETMQ_NAMESERV, "127.0.0.1:9876");
                    String voteTopic = ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_ROCKETMQ_TOPIC, "VOTE_TOPIC");
                    this.dispatcher = new RocketmqDispatcher<>(nameServAddr, voteTopic, IdUtil.instanceId(), getConfigureName(), DispatchMode.AVERAGELY, this.balanceCallback, new DBCache());
                }
            }
            this.dispatcher.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 多线程有问题，暂时不支持多线程执行，所以也不建议多个分片的场景
     */
    protected void startWorks() {
        this.shutDown = false;
        Thread thread = new Thread(() -> {
            long start;
            while (!shutDown) {
                Iterator<Map.Entry<String, ISplitReader>> it = ownerSplitReaders.entrySet().iterator();
                start = System.currentTimeMillis();
                try {
                    while (it.hasNext()) {
                        Map.Entry<String, ISplitReader> entry = it.next();
                        String splitId = entry.getKey();
                        ISplit<?, ?> split = ownerSplits.get(splitId);
                        ISplitReader reader = entry.getValue();
                        ReaderRunner runner = new ReaderRunner(split, reader);
                        runner.run();
                    }
                } catch (Exception e) {
                    LOGGER.error("[{}][{}] OpenAPI_Reader_Execute_Error", IdUtil.instanceId(), getConfigureName(), e);
                }
                waitPollingTime(start);
            }
        });
        thread.start();
    }

    @Override public Map<String, ISplit<?, ?>> getAllSplitMap() {
        List<ISplit<?, ?>> splits = fetchAllSplits();
        if (splits == null) {
            return new HashMap<>();
        }
        Map<String, ISplit<?, ?>> splitMap = new HashMap<>();
        for (ISplit<?, ?> split : splits) {
            splitMap.put(split.getQueueId(), split);
        }
        return splitMap;
    }

    @Override public List<ISplit<?, ?>> getAllSplits() {
        List<ISplit<?, ?>> splits = fetchAllSplits();
        return new ArrayList<>(splits);
    }

    protected void doSplitAddition(List<ISplit<?, ?>> changedSplits) {
        if (changedSplits == null) {
            return;
        }
        Set<String> splitIds = new HashSet<>();
        for (ISplit<?, ?> split : changedSplits) {
            if (ownerSplitReaders.containsKey(split.getQueueId())) {
                continue;
            }
            splitIds.add(split.getQueueId());
        }
        if (CollectionUtil.isEmpty(splitIds)) {
            return;
        }
        addNewSplit(splitIds);
        for (ISplit<?, ?> split : changedSplits) {
            ISplitReader reader = createSplitReader(split);
            reader.open(split);
            reader.seek(loadSplitOffset(split));
            ownerSplitReaders.put(split.getQueueId(), reader);
            this.ownerSplits.put(split.getQueueId(), split);
        }

    }

    @Override public String loadSplitOffset(ISplit<?, ?> split) {
        String offset = null;
        ISplitOffset checkPoint = checkPointManager.recover(this, split);
        if (checkPoint != null) {
            offset = checkPoint.getOffset();
        }
        return offset;
    }

    protected abstract ISplitReader createSplitReader(ISplit<?, ?> split);

    protected void doSplitRelease(List<ISplit<?, ?>> changedSplits) {

        try {
            List<SplitCloseFuture> closeFutures = new ArrayList<>();
            for (ISplit<?, ?> split : changedSplits) {
                ISplitReader reader = this.ownerSplitReaders.get(split.getQueueId());
                if (reader == null) {
                    continue;
                }
                SplitCloseFuture future = reader.close();
                closeFutures.add(future);
            }
            for (SplitCloseFuture future : closeFutures) {
                try {
                    if (!future.isDone()) {
                        future.get();
                    }
                    this.ownerSplitReaders.remove(future.getSplit().getQueueId());
                    this.ownerSplits.remove(future.getSplit().getQueueId());
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                }
            }
            LOGGER.info("[{}][{}] Source_Release_Split_StackTrace({})", IdUtil.instanceId(), getConfigureName(), JSONObject.toJSONString(Thread.currentThread().getStackTrace()));
        } catch (Exception e) {
            throw new RuntimeException("release split error", e);
        }

    }

    protected List<ISplit<?, ?>> getSplitByName(List<String> names) {
        if (CollectionUtil.isEmpty(names)) {
            return null;
        }
        Map<String, ISplit<?, ?>> allSplits = getAllSplitMap();
        List<ISplit<?, ?>> splits = new ArrayList<>();
        for (String name : names) {
            ISplit<?, ?> split = allSplits.get(name);
            if (split == null) {
                LOGGER.warn("[{}][{}] Source_Get_Split_Error({})", IdUtil.instanceId(), getConfigureName(), name);
                continue;
            }
            splits.add(split);
        }
        return splits;

    }

    protected transient Long mLastCheckTime = System.currentTimeMillis();

    protected class ReaderRunner implements Runnable {

        protected ISplit<?, ?> split;
        protected final ISplitReader reader;

        public ReaderRunner(ISplit<?, ?> split, ISplitReader reader) {
            this.split = split;
            this.reader = reader;
        }

        @Override public void run() {
            LOGGER.info("[{}][{}] Source_Start_Pull_Data", IdUtil.instanceId(), getConfigureName());
            if (!reader.isInterrupt()) {
                if (reader.next()) {
                    List<PullMessage<?>> messages = reader.getMessage();
                    if (messages != null) {
                        for (PullMessage<?> pullMessage : messages) {
                            String queueId = split.getQueueId();
                            String offset = pullMessage.getOffsetStr();
                            JSONObject msg = createJson(pullMessage.getMessage());
                            Message message = createMessage(msg, queueId, offset, false);
                            message.getHeader().setOffsetIsLong(pullMessage.getMessageOffset().isLongOfMainOffset());
                            executeMessage(message);
                        }
                    }
                }
                long curTime = System.currentTimeMillis();
                if (curTime - mLastCheckTime > getCheckpointTime()) {
                    if (!isTest) {
                        LOGGER.info("[{}][{}] Source_Save_The_Progress_On({})", IdUtil.instanceId(), getConfigureName(), reader.getProgress());
                    }
                    sendCheckpoint(reader.getSplit().getQueueId(), new MessageOffset(reader.getProgress()));
                    mLastCheckTime = curTime;
                } else {
                    if (!isTest) {
                        LOGGER.info("[{}][{}] Source_Does_Not_Save_The_Progress_On({}-{})", IdUtil.instanceId(), getConfigureName(), curTime, mLastCheckTime);
                    }
                }
            } else {
                Set<String> removeSplits = new HashSet<>();
                removeSplits.add(reader.getSplit().getQueueId());
                removeSplit(removeSplits);
                ownerSplitReaders.remove(reader.getSplit().getQueueId());
                ownerSplits.remove(reader.getSplit().getQueueId());
                reader.finish();
                synchronized (reader) {
                    reader.notifyAll();
                }
            }

        }

    }

    protected void waitPollingTime(long start) {
        try {
            long sleepTime = this.pullIntervalMs - (System.currentTimeMillis() - start);
            if (sleepTime > 0) {
                Thread.sleep(sleepTime);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override public void destroy() {
        super.destroy();
        if (this.dispatcher != null) {
            this.dispatcher.close();
        }
        doSplitRelease(new ArrayList<>(this.ownerSplits.values()));
        this.shutDown = true;
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    @Override public boolean supportNewSplitFind() {
        return true;
    }

    @Override public boolean supportRemoveSplitFind() {
        return true;
    }

    @Override public boolean supportOffsetRest() {
        return true;
    }

    @Override public Long getPullIntervalMs() {
        return pullIntervalMs;
    }

    public void setPullIntervalMs(long pullIntervalMs) {
        this.pullIntervalMs = pullIntervalMs;
    }

    @Override public List<ISplit<?, ?>> ownerSplits() {
        return new ArrayList<>(ownerSplits.values());
    }

    public boolean isTest() {
        return isTest;
    }

    public void setTest(boolean test) {
        isTest = test;
    }

    public boolean isCloseBalance() {
        return closeBalance;
    }

    public void setCloseBalance(boolean closeBalance) {
        this.closeBalance = closeBalance;
    }
}