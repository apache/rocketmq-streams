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
package org.apache.rocketmq.streams.common.topology.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.MessageGlobleTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintMetric;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.model.PipelineSourceJoiner;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * run one or multi pipeline's
 */
public class StreamsTask extends BasedConfigurable implements IStreamOperator<IMessage, AbstractContext>, IAfterConfigurableRefreshListener {
    private static final Log LOG = LogFactory.getLog(StreamsTask.class);

    public static final String TYPE = "stream_task";

    @ENVDependence
    protected String logFingerprint;
    protected int homologousRulesCaseSize = 2000000;
    protected int homologousExpressionCaseSize = 2000000;
    protected int preFingerprintCaseSize = 2000000;
    protected int parallelTasks = 4;

    /**
     * The pipeline or subtask executed in this task
     */

    protected transient List<ChainPipeline> pipelines = new ArrayList<>();
    protected List<String> pipelineNames = new ArrayList<>();
    /**
     * fingerprint cache
     */
    protected transient FingerprintCache homologousRulesCache;
    /**
     * Automatically parses pipelines, generates pre-filter fingerprints and expression estimates
     */
    protected transient IHomologousOptimization homologousOptimization;

    /**
     * Supports multi-threaded task execution
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

    public void start() {
        Map<String, Boolean> hasStart = new HashMap<>();
        while (true) {
            try {
                for (ChainPipeline pipeline : pipelines) {
                    if (hasStart.containsKey(pipeline.getConfigureName())) {
                        continue;
                    } else {
                        startPipeline(pipeline);
                        hasStart.put(pipeline.getConfigureName(), true);
                    }
                }
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public AbstractContext doMessage(IMessage message, AbstractContext context) {
        if (CollectionUtil.isEmpty(this.pipelines)) {
            return context;
        }
        if (this.homologousOptimization == null) {
            synchronized (this) {
                if (this.homologousOptimization == null) {
                    Iterable<IHomologousOptimization> iterable = ServiceLoader.load(IHomologousOptimization.class);
                    Iterator<IHomologousOptimization> it = iterable.iterator();
                    if (it.hasNext()) {
                        this.homologousOptimization = it.next();
                        this.homologousOptimization.optimizate(this.pipelines, this.homologousExpressionCaseSize, this.preFingerprintCaseSize);
                    }
                }
            }
        }
        /**
         * Calculate QPS
         */
        double qps = calculateQPS();
        if (StringUtil.isEmpty(logFingerprint)) {
            if (COUNT.get() % 1000 == 0) {
                System.out.println(getConfigureName() + " qps is " + qps + "。the count is " + COUNT.get());
            }
        }

        if (homologousOptimization != null) {
            homologousOptimization.calculate(message, context);
        }

        boolean onlyOne = this.pipelines.size() == 1;
        /**
         * Judge whether to turn on fingerprint filtering. When it is turned on, filter through fingerprint first
         */
        String msgKey = getFilterKey(message);
        BitSetCache.BitSet bitSet = getFilterValue(msgKey);
        boolean isHitCache = true;
        CountDownLatch countDownLatch = null;
        if (bitSet == null && StringUtil.isNotEmpty(this.logFingerprint)) {
            bitSet = new BitSetCache.BitSet(this.pipelines.size());
            isHitCache = false;
            if (this.parallelTasks > 0) {
                countDownLatch = new CountDownLatch(pipelines.size());
            }
        }
        int index = 0;
        for (ChainPipeline pipeline : this.pipelines) {
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
            if (countDownLatch != null && this.executorService != null) {
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
        if (StringUtil.isNotEmpty(this.logFingerprint)) {
            printQPSWithFingerprint(qps);
        }

        return null;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        List<TaskAssigner> taskAssigners = configurableService.queryConfigurableByType(TaskAssigner.TYPE);
        if (taskAssigners == null) {
            return;
        }
        boolean isChanged = false;
        List<ChainPipeline> newPipelines = new ArrayList<>();
        for (TaskAssigner taskAssigner : taskAssigners) {
            if (!getConfigureName().equals(taskAssigner.getTaskName())) {
                continue;
            }
            List<String> pipelineNames = taskAssigner.getPipelineNames();
            if (pipelineNames != null) {

                for (String pipelineName : pipelineNames) {
                    ChainPipeline pipeline = configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
                    if (pipeline != null) {
                        newPipelines.add(pipeline);
                    }
                }
            }

        }
        loadSubPipelines(newPipelines, configurableService);
        List<ChainPipeline> deletePipeline = new ArrayList<>();
        if (newPipelines.size() > 0) {
            for (ChainPipeline pipeline : newPipelines) {
                if (!this.pipelines.contains(pipeline)) {
                    isChanged = true;
                    break;
                }
            }
            for (ChainPipeline pipeline : this.pipelines) {
                if (!newPipelines.contains(pipeline)) {
                    isChanged = true;
                    deletePipeline.add(pipeline);
                }
            }
        }
        if (isChanged) {
            this.pipelines = newPipelines;
            this.homologousRulesCache = new FingerprintCache(homologousRulesCaseSize);
            for (ChainPipeline pipeline : deletePipeline) {
                pipeline.destroy();
            }
        }

        if (this.parallelTasks > 0) {
            executorService = ThreadPoolFactory.createThreadPool(this.parallelTasks);
        }
    }

    /**
     * 动态装配子pipline
     *
     * @param pipelines
     * @param configurableService
     */
    protected void loadSubPipelines(List<ChainPipeline> pipelines, IConfigurableService configurableService) {
        List<PipelineSourceJoiner> joiners = configurableService.queryConfigurableByType(PipelineSourceJoiner.TYPE);
        if (joiners == null) {
            return;
        }
        String pipelineName = getConfigureName();
        for (PipelineSourceJoiner joiner : joiners) {
            if (pipelineName.equals(joiner.getSourcePipelineName())) {
                ChainPipeline pipeline = configurableService.queryConfigurable(Pipeline.TYPE, joiner.getPipelineName());
                if (pipeline != null) {
                    pipelines.add(pipeline);
                }
            }
        }
    }

    /**
     * 如果确定这个message，在某个pipeline不触发，则记录下来，下次直接跳过，不执行
     *
     * @param msgKey
     * @param bitSet
     */
    protected void addNoFireMessage(String msgKey, BitSetCache.BitSet bitSet) {

        if (StringUtil.isEmpty(logFingerprint)) {
            return;
        }
        this.homologousRulesCache.addLogFingerprint(getOrCreateFingerNameSpace(), msgKey, bitSet);
    }

    protected BitSetCache.BitSet getFilterValue(String msgKey) {
        if (StringUtil.isEmpty(logFingerprint)) {
            return null;
        }

        return this.homologousRulesCache.getLogFingerprint(getOrCreateFingerNameSpace(), msgKey);
    }

    protected String getFilterKey(IMessage message) {

        if (StringUtil.isEmpty(logFingerprint)) {
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
        return (double)(COUNT.incrementAndGet() / second);
    }

    /**
     * When fingerprint is enabled, print QPS, filter rate, cache condition and rule matching rate
     *
     * @param qps
     */
    protected void printQPSWithFingerprint(double qps) {
        FingerprintMetric fingerprintMetric = this.homologousRulesCache.getOrCreateMetric(getOrCreateFingerNameSpace());
        double rate = fingerprintMetric.getHitCacheRate();
        double fireRate = (double)FIRE_RULE_COUNT.get() / (double)COUNT.get();
        if (COUNT.get() % 1000 == 0) {
            System.out.println(
                "qps is " + qps + ",the count is " + COUNT.get() + " the cache hit  rate " + rate
                    + " the cache size is " + fingerprintMetric.getCacheSize() + "，"
                    + "the fire rule rate is " + fireRate);
        }
    }

    /**
     * start one pipeline
     *
     * @param pipeline
     */
    protected void startPipeline(ChainPipeline pipeline) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                pipeline.startChannel();
            }
        });
        thread.start();
    }

    class HomologousTask implements Runnable {
        protected IMessage message;
        protected AbstractContext context;
        protected ChainPipeline pipeline;
        protected BitSetCache.BitSet bitSet;
        protected int index;
        protected String msgKey;
        protected CountDownLatch countDownLatch;

        public HomologousTask(IMessage message, AbstractContext context, ChainPipeline pipeline, BitSetCache.BitSet bitSet, int index, String msgKey) {
            this.message = message;
            this.context = context;
            this.pipeline = pipeline;
            this.bitSet = bitSet;
            this.index = index;
            this.msgKey = msgKey;
        }

        @Override
        public void run() {
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

    public int getParallelTasks() {
        return parallelTasks;
    }

    public void setParallelTasks(int parallelTasks) {
        this.parallelTasks = parallelTasks;
    }

    public List<ChainPipeline> getPipelines() {
        return pipelines;
    }

    public void setPipelines(List<ChainPipeline> pipelines) {
        this.pipelines = pipelines;
        for (ChainPipeline<?> pipeline : this.pipelines) {
            this.pipelineNames.add(pipeline.getConfigureName());
        }
    }

    public String getLogFingerprint() {
        return logFingerprint;
    }

    public void setLogFingerprint(String logFingerprint) {
        this.logFingerprint = logFingerprint;
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

    public List<String> getPipelineNames() {
        return pipelineNames;
    }

    public void setPipelineNames(List<String> pipelineNames) {
        this.pipelineNames = pipelineNames;
    }
}
