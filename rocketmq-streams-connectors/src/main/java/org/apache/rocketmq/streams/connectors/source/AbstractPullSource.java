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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPoint;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointManager;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.connectors.balance.ISourceBalance;
import org.apache.rocketmq.streams.connectors.balance.SplitChanged;
import org.apache.rocketmq.streams.connectors.balance.impl.LeaseBalanceImpl;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.reader.SplitCloseFuture;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

public abstract class AbstractPullSource extends AbstractSource implements IPullSource<AbstractSource> {

    private static final Log logger = LogFactory.getLog(AbstractPullSource.class);

    protected transient ISourceBalance balance;// balance interface
    protected transient ScheduledExecutorService balanceExecutor;//schdeule balance
    protected transient Map<String, ISplitReader> splitReaders = new HashMap<>();//owner split readers
    protected transient Map<String, ISplit> ownerSplits = new HashMap<>();//working splits by the source instance

    //可以有多种实现，通过名字选择不同的实现
    protected String balanceName = LeaseBalanceImpl.DB_BALANCE_NAME;
    //balance schedule time
    protected int balanceTimeSecond = 10;
    protected long pullIntervalMs;
    protected transient CheckPointManager checkPointManager = new CheckPointManager();
    protected transient boolean shutDown=false;
    @Override
    protected boolean startSource() {
        ServiceLoaderComponent serviceLoaderComponent = ServiceLoaderComponent.getInstance(ISourceBalance.class);
        balance = (ISourceBalance) serviceLoaderComponent.getService().loadService(balanceName);
        balance.setSourceIdentification(MapKeyUtil.createKey(getNameSpace(), getConfigureName()));
        balanceExecutor = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().namingPattern("balance-task-%d").daemon(true).build());
        List<ISplit> allSplits = fetchAllSplits();
        SplitChanged splitChanged = balance.doBalance(allSplits, new ArrayList(ownerSplits.values()));
        doSplitChanged(splitChanged);
        balanceExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logger.info("balance running..... current splits is " + ownerSplits);
                List<ISplit> allSplits = fetchAllSplits();
                SplitChanged splitChanged = balance.doBalance(allSplits, new ArrayList(ownerSplits.values()));
                doSplitChanged(splitChanged);
            }
        }, balanceTimeSecond, balanceTimeSecond, TimeUnit.SECONDS);

        startWorks();
        return true;
    }

    private void startWorks() {
        ExecutorService workThreads= ThreadPoolFactory.createThreadPool(maxThread);
        long start=System.currentTimeMillis();
        while (!shutDown) {
            Iterator<Map.Entry<String, ISplitReader>> it = splitReaders.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, ISplitReader> entry=it.next();
                String splitId=entry.getKey();
                ISplit split=ownerSplits.get(splitId);
                ISplitReader reader=entry.getValue();
                ReaderRunner runner=new ReaderRunner(split,reader);
                workThreads.execute(runner);
            }
            try {
                long sleepTime=this.pullIntervalMs-(System.currentTimeMillis()-start);
                if(sleepTime>0){
                    Thread.sleep(sleepTime);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public Map<String, ISplit> getAllSplitMap() {
        List<ISplit> splits = fetchAllSplits();
        if (splits == null) {
            return new HashMap<>();
        }
        Map<String, ISplit> splitMap = new HashMap<>();
        for (ISplit split : splits) {
            splitMap.put(split.getQueueId(), split);
        }
        return splitMap;
    }

    protected void doSplitChanged(SplitChanged splitChanged) {
        if (splitChanged == null) {
            return;
        }
        if (splitChanged.getSplitCount() == 0) {
            return;
        }
        if (splitChanged.isNewSplit()) {
            doSplitAddition(splitChanged.getChangedSplits());
        } else {
            doSplitRelease(splitChanged.getChangedSplits());
        }
    }

    protected void doSplitAddition(List<ISplit> changedSplits) {
        if (changedSplits == null) {
            return;
        }
        Set<String> splitIds = new HashSet<>();
        for (ISplit split : changedSplits) {
            splitIds.add(split.getQueueId());
        }
        addNewSplit(splitIds);
        for (ISplit split : changedSplits) {
            ISplitReader reader = createSplitReader(split);
            reader.open(split);
            reader.seek(loadSplitOffset(split));
            splitReaders.put(split.getQueueId(), reader);
            this.ownerSplits.put(split.getQueueId(), split);
//            logger.info("start next");
//            Thread thread = new Thread(new Runnable() {
//
//            thread.setName("reader-task-" + reader.getSplit().getQueueId());
//            thread.start();
        }

    }

    @Override
    public String loadSplitOffset(ISplit split) {
        String offset = null;
        CheckPoint<String> checkPoint = checkPointManager.recover(this, split);
        if (checkPoint != null) {
            offset = JSON.parseObject(checkPoint.getData()).getString("offset");
        }
        return offset;
    }

    protected abstract ISplitReader createSplitReader(ISplit split);

    protected void doSplitRelease(List<ISplit> changedSplits) {
        boolean success = balance.getRemoveSplitLock();
        if (!success) {
            return;
        }
        try {
            List<SplitCloseFuture> closeFutures = new ArrayList<>();
            for (ISplit split : changedSplits) {
                ISplitReader reader = this.splitReaders.get(split.getQueueId());
                if (reader == null) {
                    continue;
                }
                SplitCloseFuture future = reader.close();
                closeFutures.add(future);
            }
            for (SplitCloseFuture future : closeFutures) {
                try {
                    if(!future.isDone()){
                        future.get();
                    }
                    this.splitReaders.remove(future.getSplit().getQueueId());
                    this.ownerSplits.remove(future.getSplit().getQueueId());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

        } finally {
            balance.unLockRemoveSplitLock();
        }

    }


    protected class ReaderRunner implements Runnable{
        long mLastCheckTime = System.currentTimeMillis();
        protected ISplit split;
        protected ISplitReader reader;

        public ReaderRunner(ISplit split,ISplitReader reader){
            this.split=split;
            this.reader=reader;
        }

        @Override
        public void run() {
            logger.info("start running");
            if (reader.isInterrupt() == false) {
                if (reader.next()) {
                    List<PullMessage> messages = reader.getMessage();
                    if (messages != null) {
                        for (PullMessage pullMessage : messages) {
                            String queueId = split.getQueueId();
                            String offset = pullMessage.getOffsetStr();
                            JSONObject msg = createJson(pullMessage.getMessage());
                            Message message = createMessage(msg, queueId, offset, false);
                            message.getHeader().setOffsetIsLong(pullMessage.getMessageOffset().isLongOfMainOffset());
                            executeMessage(message);
                        }
                    }
                    reader.notifyAll();
                }
                long curTime = System.currentTimeMillis();
                if (curTime - mLastCheckTime > getCheckpointTime()) {
                    sendCheckpoint(reader.getSplit().getQueueId());
                    mLastCheckTime = curTime;
                }


            }else {
                Set<String> removeSplits = new HashSet<>();
                removeSplits.add(reader.getSplit().getQueueId());
                removeSplit(removeSplits);
                balance.unlockSplit(split);
                reader.close();
                synchronized (reader) {
                    reader.notifyAll();
                }
            }

        }

    }

    @Override
    public boolean supportNewSplitFind() {
        return true;
    }

    @Override
    public boolean supportRemoveSplitFind() {
        return true;
    }

    @Override
    public boolean supportOffsetRest() {
        return true;
    }

    @Override
    public Long getPullIntervalMs() {
        return pullIntervalMs;
    }

    public String getBalanceName() {
        return balanceName;
    }

    public void setBalanceName(String balanceName) {
        this.balanceName = balanceName;
    }

    public int getBalanceTimeSecond() {
        return balanceTimeSecond;
    }

    public void setBalanceTimeSecond(int balanceTimeSecond) {
        this.balanceTimeSecond = balanceTimeSecond;
    }

    public void setPullIntervalMs(long pullIntervalMs) {
        this.pullIntervalMs = pullIntervalMs;
    }

    @Override
    public List<ISplit> ownerSplits() {
        return new ArrayList(ownerSplits.values());
    }

}