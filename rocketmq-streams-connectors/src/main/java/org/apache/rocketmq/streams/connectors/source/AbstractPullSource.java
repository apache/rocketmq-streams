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

import java.util.*;
import java.util.concurrent.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointManager;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.connectors.balance.ISourceBalance;
import org.apache.rocketmq.streams.connectors.balance.SplitChanged;
import org.apache.rocketmq.streams.connectors.balance.impl.LeaseBalanceImpl;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPoint;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
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

    transient CheckPointManager checkPointManager = new CheckPointManager();

    protected transient ScheduledFuture scheduledFuture;

    @Override
    protected boolean startSource() {
        ServiceLoaderComponent serviceLoaderComponent = ServiceLoaderComponent.getInstance(ISourceBalance.class);
        balance = (ISourceBalance)serviceLoaderComponent.getService().loadService(balanceName);
        balance.setSourceIdentification(MapKeyUtil.createKey(getNameSpace(),getConfigureName()));
        balanceExecutor = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().namingPattern("balance-task-%d").daemon(true).build());
        List<ISplit> allSplits = fetchAllSplits();
        SplitChanged splitChanged = balance.doBalance(allSplits,new ArrayList(ownerSplits.values()));
        doSplitChanged(splitChanged);
        scheduledFuture = balanceExecutor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                logger.info("balance running..... current splits is " + ownerSplits);
                List<ISplit> allSplits = fetchAllSplits();
                SplitChanged splitChanged = balance.doBalance(allSplits, new ArrayList(ownerSplits.values()));
                doSplitChanged(splitChanged);
            }
        }, balanceTimeSecond, balanceTimeSecond, TimeUnit.SECONDS);
        return true;
    }


    @Override
    public Map<String, ISplit> getAllSplitMap() {
        List<ISplit> splits = fetchAllSplits();
        if(splits == null){
            return new HashMap<>();
        }
        Map<String, ISplit> splitMap = new HashMap<>();
        for(ISplit split:splits){
            splitMap.put(split.getQueueId(),split);
        }
        return splitMap;
    }

    protected void doSplitChanged(SplitChanged splitChanged) {
        if(splitChanged == null){
            return;
        }
        if(splitChanged.getSplitCount() == 0){
            return;
        }
        if(splitChanged.isNewSplit()){
            doSplitAddition(splitChanged.getChangedSplits());
        }else {
            doSplitRelease(splitChanged.getChangedSplits());
        }
    }


    protected synchronized void doSplitAddition(List<ISplit> changedSplits) {
        if(changedSplits == null){
            return;
        }
        Set<String> splitIds = new HashSet<>();
        for(ISplit split:changedSplits){
            splitIds.add(split.getQueueId());
        }
        addNewSplit(splitIds);
        for(ISplit split : changedSplits){
            ISplitReader reader = createSplitReader(split);
            reader.open(split);
            reader.seek(loadSplitOffset(split));
            splitReaders.put(split.getQueueId(), reader);
            this.ownerSplits.put(split.getQueueId(), split);
            logger.info("start next");
            Thread thread = new Thread(new Runnable() {
                long mLastCheckTime = System.currentTimeMillis();
                @Override
                public void run() {
                    logger.info("start running");
                    while (reader.isInterrupt() == false){
//                        logger.info("start running while");

                        if(reader.next()){

//                            logger.info("start running while next");

                            List<PullMessage> messages = reader.getMessage();
                            if(messages != null){
                                for(PullMessage pullMessage:messages){
                                    String queueId = split.getQueueId();
                                    String offset = pullMessage.getOffsetStr();
                                    JSONObject msg = createJson(pullMessage.getMessage());
                                    Message message = createMessage(msg, queueId, offset,false);
                                    message.getHeader().setOffsetIsLong(pullMessage.getMessageOffset().isLongOfMainOffset());
                                    executeMessage(message);
                                }
                            }
                        }
                        long curTime = System.currentTimeMillis();
                        if (curTime - mLastCheckTime > getCheckpointTime()) {
                            sendCheckpoint(reader.getSplit().getQueueId());
                            mLastCheckTime = curTime;
                        }
                        try {
                            Thread.sleep(pullIntervalMs);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }

                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    Set<String> removeSplits = new HashSet<>();
                    removeSplits.add(reader.getSplit().getQueueId());
                    removeSplit(removeSplits);
                    balance.unlockSplit(split);
                    reader.close();
                    synchronized (reader){
                        reader.notifyAll();
                    }

                }
            });
            thread.setName("reader-task-" + reader.getSplit().getQueueId());
            thread.start();
        }

    }

    @Override
    public void finish(){
        closeReader();
        scheduledFuture.cancel(true);
        balanceExecutor.shutdown();
        super.finish();
    }

    @Override
    public String loadSplitOffset(ISplit split) {
//        return CheckPoint.loadOffset(this,split.getQueueId());
        String offset = null;
        CheckPoint<String> checkPoint = checkPointManager.recover(this, split);
        if(checkPoint != null){
            offset = JSON.parseObject(checkPoint.getData()).getString("offset");
        }
        return offset;
    }

    protected abstract ISplitReader createSplitReader(ISplit split);

    protected synchronized void closeReader(){
        Iterator<Map.Entry<String, ISplit>> it = this.ownerSplits.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry<String, ISplit> next = it.next();
            String key = next.getKey();
            this.splitReaders.get(key).interrupt();
        }
        ownerSplits.clear();
        splitReaders.clear();
    }

    protected synchronized void doSplitRelease(List<ISplit> changedSplits) {
        boolean success = balance.getRemoveSplitLock();
        if(!success){
            return;
        }
        try {
            List<SplitCloseFuture> closeFutures = new ArrayList<>();
            for(ISplit split : changedSplits){
                ISplitReader reader = this.splitReaders.get(split.getQueueId());
                if(reader == null){
                    continue;
                }
                SplitCloseFuture future = reader.close();
                closeFutures.add(future);
            }
            for(SplitCloseFuture future : closeFutures){
                try {
                    future.get();
                    this.splitReaders.remove(future.getSplit().getQueueId());
                    this.ownerSplits.remove(future.getSplit().getQueueId());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }

        }finally {
            balance.unLockRemoveSplitLock();
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


