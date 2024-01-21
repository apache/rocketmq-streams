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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.AbstractCheckPointStorage;
import org.apache.rocketmq.streams.common.checkpoint.ICheckPointStorage;
import org.apache.rocketmq.streams.common.checkpoint.ISplitOffset;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.client.SplitConsumer;
import org.apache.rocketmq.streams.dispatcher.ICache;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;
import org.apache.rocketmq.streams.dispatcher.cache.DBCache;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.impl.LeaseDispatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractPullSource extends AbstractSource implements IPullSource<AbstractSource> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractPullSource.class);
    protected static final String TASK_NAME_SPLIT = "->";
    protected final String START_TIME = "start_time";
    /**
     * 可以有多种实现，通过名字选择不同的实现
     */
    protected long pullIntervalMs = 1000 * 20;
    /**
     * 每次拉取的条数
     */
    protected int pullSize = 1000;
    protected transient volatile boolean shutDown = false;
    protected boolean isTest = false;
    protected boolean closeBalance = false;
    protected transient Map<String, SplitConsumer> ownerConsumers = new ConcurrentHashMap<>() {};
    protected transient ICache cache;
    private transient LeaseDispatcher<?> dispatcher;
    private transient IDispatcherCallback<?> balanceCallback;
    private transient ExecutorService executorService;

    @Override
    protected boolean initConfigurable() {
        if (this.cache == null) {
            this.cache = new DBCache();
        }
        if (this.executorService == null) {
            this.executorService = ThreadPoolFactory.createThreadPool(10, this.getNameSpace() + "_" + this.getName());
        }
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
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

    protected void startWorks() {
        this.shutDown = false;
        Thread thread = new Thread(() -> {
            long start;
            while (!shutDown) {
                start = System.currentTimeMillis();
                try {
                    if (this.ownerConsumers != null && !this.ownerConsumers.isEmpty()) {
                        for (SplitConsumer consumer : ownerConsumers.values()) {
                            consumer.consume();
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error("[{}][{}] Pull_Reader_Execute_Error", IdUtil.instanceId(), getName(), e);
                }
                waitPollingTime(start);
            }
        });
        thread.start();
    }

    @Override
    public Map<String, ISplit<?, ?>> getAllSplitMap() {
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

    public List<ISplit<?, ?>> getAllSplits() {
        List<ISplit<?, ?>> splits = fetchAllSplits();
        return new ArrayList<>(splits);
    }

    protected synchronized void doSplitAddition(List<ISplit<?, ?>> changedSplits) {
        if (changedSplits == null) {
            return;
        }
        for (ISplit<?, ?> split : changedSplits) {
            if (ownerConsumers.containsKey(split.getQueueId())) {
                continue;
            }
            SplitConsumer splitConsumer = new SplitConsumer(this, split, executorService);
            splitConsumer.open();
            ownerConsumers.put(split.getQueueId(), splitConsumer);
        }
    }

    protected synchronized void doSplitRelease(List<ISplit<?, ?>> changedSplits) {

        try {
            for (ISplit<?, ?> split : changedSplits) {
                SplitConsumer consumer = this.ownerConsumers.get(split.getQueueId());
                if (consumer == null) {
                    continue;
                }
                consumer.destroy();
                this.ownerConsumers.remove(split.getQueueId());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public String loadSplitOffset(ISplit<?, ?> split) {
        String offset = null;
        ISplitOffset checkPoint = checkPointManager.recover(this, split);
        if (checkPoint != null) {
            offset = checkPoint.getOffset();
        }
        return offset;
    }

    public ISplitReader createReader(ISplit<?, ?> split) {
        return createSplitReader(split);
    }

    protected abstract ISplitReader createSplitReader(ISplit<?, ?> split);

    protected List<ISplit<?, ?>> getSplitByName(List<String> names) {
        if (CollectionUtil.isEmpty(names)) {
            return null;
        }
        Map<String, ISplit<?, ?>> allSplits = getAllSplitMap();
        List<ISplit<?, ?>> splits = new ArrayList<>();
        for (String name : names) {
            ISplit<?, ?> split = allSplits.get(name);
            if (split == null) {
                LOGGER.warn("[{}][{}] Source_Get_Split_Error({})", IdUtil.instanceId(), getName(), name);
                continue;
            }
            splits.add(split);
        }
        return splits;

    }

    protected void setOffsetStore() {
        ICheckPointStorage iCheckPointStorage = new AbstractCheckPointStorage() {

            @Override
            public String getStorageName() {
                return "db";
            }

            @Override
            public <T extends ISplitOffset> void save(List<T> checkPointState) {
                if (checkPointState == null) {
                    return;
                }
                for (T splitOffset : checkPointState) {
                    String namespace = MapKeyUtil.createKey(getNameSpace(), getName());
                    String queueId = splitOffset.getQueueId();
                    String offset = splitOffset.getOffset();
                    cache.putKeyConfig(namespace, queueId, offset);
                }

            }

            @Override
            public ISplitOffset recover(ISource iSource, String queueID) {
                String name = MapKeyUtil.createKey(iSource.getNameSpace(), iSource.getName());
                String offset = cache.getKeyConfig(name, queueID);
                return new ISplitOffset() {

                    @Override
                    public String getName() {
                        return name;
                    }

                    @Override
                    public String getQueueId() {
                        return queueID;
                    }

                    @Override
                    public String getOffset() {
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
                this.balanceCallback = new IDispatcherCallback<>() {

                    @Override
                    public List<String> start(List<String> jobNames) {
                        List<String> names = splitQueueIds(jobNames);
                        List<ISplit<?, ?>> additionSplits = getSplitByName(names);
                        if (CollectionUtil.isEmpty(additionSplits)) {
                            return jobNames;
                        }
                        doSplitAddition(additionSplits);
                        return jobNames;
                    }

                    @Override
                    public List<String> stop(List<String> jobNames) {
                        List<String> names = splitQueueIds(jobNames);
                        List<ISplit<?, ?>> additionSplits = getSplitByName(names);
                        if (CollectionUtil.isEmpty(additionSplits)) {
                            return jobNames;
                        }
                        doSplitRelease(additionSplits);
                        return jobNames;
                    }

                    @Override
                    public List<String> list() {
                        List<ISplit<?, ?>> splits = getAllSplits();
                        List<String> names = new ArrayList<>();
                        for (ISplit<?, ?> split : splits) {
                            names.add(getName() + TASK_NAME_SPLIT + split.getQueueId());
                        }
                        return names;
                    }

                    @Override
                    public void destroy() {

                    }
                };
            }
            if (this.dispatcher == null) {
                String jdbc = getConfiguration().getProperty(ConfigurationKey.JDBC_DRIVER);
                String url = getConfiguration().getProperty(ConfigurationKey.JDBC_URL);
                String userName = getConfiguration().getProperty(ConfigurationKey.JDBC_USERNAME);
                String password = getConfiguration().getProperty(ConfigurationKey.JDBC_PASSWORD);
                int scheduleTime = Integer.parseInt(getConfiguration().getProperty(ConfigurationKey.DIPPER_DISPATCHER_SCHEDULE_TIME, "60"));
                this.dispatcher = new LeaseDispatcher<>(jdbc, url, userName, password, IdUtil.workerId(), getNameSpace() + "_" + getName(), DispatchMode.AVERAGELY, scheduleTime, this.balanceCallback, new DBCache(), "partition");
            }
            this.dispatcher.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
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

    @Override
    public void destroySource() {
        if (this.dispatcher != null) {
            this.dispatcher.close();
        }
        List<ISplit<?, ?>> splits = new ArrayList<>();
        for (SplitConsumer splitConsumer : this.ownerConsumers.values()) {
            splits.add(splitConsumer.getSplit());
        }
        doSplitRelease(new ArrayList<>(splits));
        this.ownerConsumers = new HashMap<>();
        this.shutDown = true;
    }

    @Override
    public Long getPullIntervalMs() {
        return pullIntervalMs;
    }

    public void setPullIntervalMs(long pullIntervalMs) {
        this.pullIntervalMs = pullIntervalMs;
    }

    @Override
    public List<ISplit<?, ?>> ownerSplits() {
        List<ISplit<?, ?>> splits = new ArrayList<>();
        for (SplitConsumer splitConsumer : this.ownerConsumers.values()) {
            splits.add(splitConsumer.getSplit());
        }
        return splits;
    }

    protected List<String> splitQueueIds(List<String> taskNames) {
        if (taskNames == null) {
            return new ArrayList<>();
        }
        List<String> names = new ArrayList<>();
        for (String taskName : taskNames) {
            names.add(taskName.split(TASK_NAME_SPLIT)[1]);
        }
        return names;
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

    public int getPullSize() {
        return pullSize;
    }

    public void setPullSize(int pullSize) {
        this.pullSize = pullSize;
    }
}