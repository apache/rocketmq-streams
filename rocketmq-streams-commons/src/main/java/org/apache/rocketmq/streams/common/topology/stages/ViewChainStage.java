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
package org.apache.rocketmq.streams.common.topology.stages;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.MessageGlobleTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintMetric;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.task.TaskAssigner;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class ViewChainStage<T extends IMessage> extends OutputChainStage<T> implements IAfterConfigurableRefreshListener {
    private static final Log LOG = LogFactory.getLog(ViewChainStage.class);

    /**
     * 动态加载的pipeline，pipeline的source type是view，且tablename=sink的view name
     */
    protected transient List<ChainPipeline<?>> pipelines = new ArrayList<>();
    /**
     * 是否开启优化
     */
    protected transient Boolean isOpenOptimization = true;

    /**
     * Used for fingerprint filtering
     */
    @ENVDependence protected String logFingerprint;
    @ENVDependence protected String logFingerprintSwitch;
    /**
     * Homologous expression result cache
     */
    protected int homologousRulesCaseSize = 2000000;
    protected int homologousExpressionCaseSize = 2000000;

    /**
     * Pre fingerprint filtering
     */
    protected int preFingerprintCaseSize = 2000000;
    protected int parallelTasks = 4;
    /**
     * fingerprint cache
     */
    protected transient FingerprintCache homologousRulesCache;
    /**
     * Automatically parses pipelines, generates pre-filter fingerprints and expression estimates
     */
    protected transient IHomologousOptimization homologousOptimization;

    /**
     * Supports mul-threaded task execution
     */
    protected transient ExecutorService executorService;
    /**
     * 总处理数据数
     */
    private final transient AtomicLong COUNT = new AtomicLong(0);
    /**
     * 触发规则的个数
     */
    private final transient AtomicLong FIRE_RULE_COUNT = new AtomicLong(0);
    /**
     * 最早的处理时间
     */
    protected transient Long firstReceiveTime = null;



    /**
     * 是否包含key by节点
     */
    protected transient boolean isContainsKeyBy = false;

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            if (CollectionUtil.isEmpty(pipelines)) {
                return null;
            }

            if (homologousOptimization == null && isOpenOptimization) {
                synchronized (this) {
                    if (homologousOptimization == null) {
                        String isOpenOptimizationStr = ComponentCreator.getProperties().getProperty("homologous.optimization.switch");
                        boolean isOpenOptimization = true;
                        if (StringUtil.isNotEmpty(isOpenOptimizationStr)) {
                            isOpenOptimization = Boolean.parseBoolean(isOpenOptimizationStr);
                        }
                        isOpenOptimization = isOpenOptimization;
                        if (isOpenOptimization) {
                            Iterable<IHomologousOptimization> iterable = ServiceLoader.load(IHomologousOptimization.class);
                            Iterator<IHomologousOptimization> it = iterable.iterator();
                            if (it.hasNext()) {
                                homologousOptimization = it.next();
                                homologousOptimization.optimizate(pipelines, homologousExpressionCaseSize, preFingerprintCaseSize);
                            }
                        }
                    }
                }
            }
            //Calculate QPS
            double qps = calculateQPS();
            if (StringUtil.isEmpty(logFingerprint)) {
                if (COUNT.get() % 10000 == 0) {
                    System.out.println(getConfigureName() + " qps is " + qps + "。the count is " + COUNT.get());
                }
            }

            if (homologousOptimization != null) {
                homologousOptimization.calculate(message, context);
            }

            boolean onlyOne = pipelines.size() == 1;
            //Judge whether to turn on fingerprint filtering. When it is turned on, filter through fingerprint first
            String msgKey = getFilterKey(message);
            BitSetCache.BitSet bitSet = getFilterValue(msgKey);
            boolean isHitCache = true;
            CountDownLatch countDownLatch = null;
            if (bitSet == null && StringUtil.isNotEmpty(logFingerprint)&&StringUtil.isNotEmpty(logFingerprintSwitch)&&Boolean.valueOf(logFingerprintSwitch)) {
                bitSet = new BitSetCache.BitSet(pipelines.size());
                isHitCache = false;
                if (!isContainsKeyBy && pipelines.size() > 1 && parallelTasks > 1) {
                    countDownLatch = new CountDownLatch(pipelines.size());
                }
            }
            int index = 0;
            for (ChainPipeline<?> pipeline :pipelines) {
                //If the fingerprint matches, filter it directly
                if (isHitCache && bitSet != null && bitSet.get(index)) {
                    index++;
                    continue;
                }

                IMessage copyMessage = message;
                if (!onlyOne) {
                    copyMessage = message.deepCopy();
                }
                Context newContext = new Context(copyMessage);
                newContext.setHomologousResult(context.getHomologousResult());
                newContext.setQuickFilterResult(context.getQuickFilterResult());
                HomologousTask homologousTask = new HomologousTask(copyMessage, newContext, pipeline, bitSet, index, msgKey);
                if (executorService != null && countDownLatch != null) {
                    homologousTask.setCountDownLatch(countDownLatch);
                    executorService.execute(homologousTask);
                } else {
                    homologousTask.run();
                }
                index++;
            }

            if (countDownLatch != null) {
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (!isHitCache) {
                addNoFireMessage(msgKey, bitSet);
            }
            if (StringUtil.isNotEmpty(logFingerprint)) {
                printQPSWithFingerprint(qps);
            }

            return null;
        }

        @Override
        public String getName() {
            return ViewChainStage.class.getName();
        }
    };

    @Override
    public boolean isAsyncNode() {
        for (Pipeline pipline : this.pipelines) {
            if (pipline.isAsynNode() == true) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        sendSystem(message, context, this.pipelines);
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        sendSystem(message, context, this.pipelines);
    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        sendSystem(message, context, this.pipelines);
    }

    @Override
    public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage) {
        sendSystem(message, context, this.pipelines);
    }

    /**
     * 每隔一段时间会重新刷新数据，如果有新增的pipline会加载起来，如果有删除的会去除掉
     *
     * @param configurableService
     */
    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        List<ChainPipeline<?>> newPipelines = loadSubPipelines(configurableService);
        boolean isChanged = false;
        if(newPipelines==null){
            return;
        }


        List<ChainPipeline<?>> deletePipeline = new ArrayList<>();
        if (newPipelines.size() > 0) {
            for (ChainPipeline<?> pipeline : newPipelines) {
                if (!this.pipelines.contains(pipeline)) {
                    isChanged = true;
                    break;
                }
            }
            for (ChainPipeline<?> pipeline : this.pipelines) {
                if (!newPipelines.contains(pipeline)) {
                    isChanged = true;
                    deletePipeline.add(pipeline);
                }
            }
        }

        if (isChanged) {
            this.pipelines = newPipelines;
            this.homologousRulesCache = new FingerprintCache(homologousRulesCaseSize);
            for (ChainPipeline<?> pipeline : deletePipeline) {
                pipeline.destroy();
            }
        }

        if (this.parallelTasks > 0) {
            executorService = ThreadPoolFactory.createThreadPool(this.parallelTasks);
        }
    }
    @Override
    public void setSink(ISink channel) {
        ViewSink viewSink=(ViewSink)channel;
        this.sink = channel;
        this.setNameSpace(channel.getNameSpace());
        this.setSinkName(channel.getConfigureName());
        this.setLabel(channel.getConfigureName());
        this.setConfigureName(viewSink.getViewTableName());
    }
    /**
     * 动态装配子pipeline
     *
     * @param configurableService configurableService
     */
    protected List<ChainPipeline<?>> loadSubPipelines(IConfigurableService configurableService) {
        List<TaskAssigner> taskAssigners = configurableService.queryConfigurableByType(TaskAssigner.TYPE);
        if (taskAssigners == null) {
            return null;
        }
        String taskName = getConfigureName();
        List<ChainPipeline<?>> subPipelines=new ArrayList<>();
        for (TaskAssigner taskAssigner : taskAssigners) {
            if (!taskName.equals(taskAssigner.getTaskName())) {
                continue;
            }
            String pipelineName = taskAssigner.getPipelineName();
            if(pipelineName!=null){
                ChainPipeline<?> pipeline = configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
                if (pipeline != null) {
                    subPipelines.add(pipeline);
                }
            }
        }
        return subPipelines;
    }




    /**
     * When fingerprint is enabled, print QPS, filter rate, cache condition and rule matching rate
     *
     * @param qps qps
     */
    protected void printQPSWithFingerprint(double qps) {
        FingerprintMetric fingerprintMetric = this.homologousRulesCache.getOrCreateMetric(getOrCreateFingerNameSpace());
        double rate = fingerprintMetric.getHitCacheRate();
        double fireRate = (double) FIRE_RULE_COUNT.get() / (double) COUNT.get();
        if (COUNT.get() % 1000 == 0) {
            System.out.println("qps is " + qps + ",the count is " + COUNT.get() + " the cache hit  rate " + rate + " the cache size is " + fingerprintMetric.getCacheSize() + "，" + "the fire rule rate is " + fireRate);
        }
    }


    /**
     * 如果确定这个message，在某个pipeline不触发，则记录下来，下次直接跳过，不执行
     *
     * @param msgKey msgKey
     * @param bitSet bitSet
     */
    protected void addNoFireMessage(String msgKey, BitSetCache.BitSet bitSet) {

        if (StringUtil.isEmpty(logFingerprint)) {
            return;
        }
        if(StringUtil.isEmpty(logFingerprintSwitch)){
            return ;
        }

        if(Boolean.valueOf(logFingerprintSwitch)==false){
            return ;
        }
        this.homologousRulesCache.addLogFingerprint(getOrCreateFingerNameSpace(), msgKey, bitSet);
    }

    protected BitSetCache.BitSet getFilterValue(String msgKey) {
        if (StringUtil.isEmpty(logFingerprint)) {
            return null;
        }
        if(StringUtil.isEmpty(logFingerprintSwitch)){
            return null;
        }

        if(Boolean.valueOf(logFingerprintSwitch)==false){
            return null;
        }
        return this.homologousRulesCache.getLogFingerprint(getOrCreateFingerNameSpace(), msgKey);
    }

    protected String getFilterKey(IMessage message) {

        if (StringUtil.isEmpty(logFingerprint)) {
            return null;
        }

        if(StringUtil.isEmpty(logFingerprintSwitch)){
            return null;
        }

        if(Boolean.valueOf(logFingerprintSwitch)==false){
            return null;
        }

        return FingerprintCache.creatFingerpringKey(message, getOrCreateFingerNameSpace(), this.logFingerprint);
    }



    protected String getOrCreateFingerNameSpace() {
        return getConfigureName();
    }

    /**
     * Print QPS in the scene without fingerprint on
     */
    protected double calculateQPS() {
        if (firstReceiveTime == null) {
            synchronized (this) {
                if (firstReceiveTime == null) {
                    firstReceiveTime = System.currentTimeMillis();
                }
            }
        }
        long second = ((System.currentTimeMillis() - firstReceiveTime) / 1000);
        if (second == 0) {
            second = 1;
        }
        return (double) (COUNT.incrementAndGet() / second);
    }
    /**
     * 把消息投递给pipline的channel，让子pipline完成任务 注意：子pipline对消息的任何修改，都不反映到当前的pipline
     *
     * @param t
     * @param context
     * @return
     */
    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }



    class HomologousTask implements Runnable {
        protected IMessage message;
        protected AbstractContext context;
        protected ChainPipeline pipeline;
        protected BitSetCache.BitSet bitSet;
        protected int index;
        protected String msgKey;
        protected CountDownLatch countDownLatch;

        public HomologousTask(IMessage message, AbstractContext context, ChainPipeline<?> pipeline, BitSetCache.BitSet bitSet, int index, String msgKey) {
            this.message = message;
            this.context = context;
            this.pipeline = pipeline;
            this.bitSet = bitSet;
            this.index = index;
            this.msgKey = msgKey;
        }

        @Override public void run() {
            try {
                pipeline.doMessage(message, context);
                Boolean isFinish = MessageGlobleTrace.existFinishBranch(message);
                if (isFinish != null && !isFinish) {
                    if (bitSet != null) {
                        bitSet.set(index);
                    }
                }
                Boolean existFinishBranch = MessageGlobleTrace.existFinishBranch(message);
                if (existFinishBranch != null && existFinishBranch) {
                    FIRE_RULE_COUNT.incrementAndGet();
                }
            } catch (Exception e) {
                e.printStackTrace();
                LOG.error("pipeline execute error " + pipeline.getConfigureName(), e);
            } finally {
                if (this.countDownLatch != null) {
                    this.countDownLatch.countDown();
                }
            }
        }

        public CountDownLatch getCountDownLatch() {
            return countDownLatch;
        }

        public void setCountDownLatch(CountDownLatch countDownLatch) {
            this.countDownLatch = countDownLatch;
        }
    }

    public int getHomologousRulesCaseSize() {
        return homologousRulesCaseSize;
    }

    public void setHomologousRulesCaseSize(int homologousRulesCaseSize) {
        this.homologousRulesCaseSize = homologousRulesCaseSize;
    }

    public int getHomologousExpressionCaseSize() {
        return homologousExpressionCaseSize;
    }

    public void setHomologousExpressionCaseSize(int homologousExpressionCaseSize) {
        this.homologousExpressionCaseSize = homologousExpressionCaseSize;
    }

    public int getPreFingerprintCaseSize() {
        return preFingerprintCaseSize;
    }

    public void setPreFingerprintCaseSize(int preFingerprintCaseSize) {
        this.preFingerprintCaseSize = preFingerprintCaseSize;
    }

    public int getParallelTasks() {
        return parallelTasks;
    }

    public void setParallelTasks(int parallelTasks) {
        this.parallelTasks = parallelTasks;
    }

    public String getLogFingerprint() {
        return logFingerprint;
    }

    public void setLogFingerprint(String logFingerprint) {
        this.logFingerprint = logFingerprint;
    }

    public String getLogFingerprintSwitch() {
        return logFingerprintSwitch;
    }

    public void setLogFingerprintSwitch(String logFingerprintSwitch) {
        this.logFingerprintSwitch = logFingerprintSwitch;
    }
}
