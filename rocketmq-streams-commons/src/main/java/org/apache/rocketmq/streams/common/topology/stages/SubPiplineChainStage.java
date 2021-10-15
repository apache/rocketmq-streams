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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
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
import org.apache.rocketmq.streams.common.optimization.LogFingerprintFilter;
import org.apache.rocketmq.streams.common.optimization.MessageGloableTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintMetric;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.model.PipelineSourceJoiner;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class
SubPiplineChainStage<T extends IMessage> extends ChainStage<T> implements IAfterConfigurableRefreshListener {
    private static String INNER_MESSAGE = "inner_message";//保存原始信息，用于收集触发规则的原始日志时，使用
    private static String FIRE_RUEL = "fireRule";//触发的规则
    private static String FIRE_RULE_OUT_FILE_NAME = "dipper.msg";//触发的规则和原始数据，输出的文件名

    private static final Log LOG = LogFactory.getLog(SubPiplineChainStage.class);
    public static final String ORI_MESSAGE_KEY = "__ori_msg";//保留一份原始消息

    @Deprecated
    protected String piplineNameRegex;//只要pipline的命名符合这个正则逻辑，则会被这个stage处理。可以为null
    @Deprecated
    protected List<String> piplineNames;//指定要处理的pipline 名字。可以为null
    protected transient List<ChainPipeline> piplines = new ArrayList<>();
    protected transient volatile boolean startQPSCount = true;//是否启动qps的统计
    private transient AtomicLong COUNT = new AtomicLong(0);//总处理数据数
    private transient AtomicLong FILTER_COUNT = new AtomicLong(0);//总处理数据数
    private transient AtomicLong FIRE_RULE_COUNT = new AtomicLong(0);//触发规则的个数
    protected transient Long firstReceiveTime = null;//最早的处理时间

    /**
     * 过滤器会通过反射创建，需要无参，创建后对象是AbstractMessageRepeateFileter
     */
    protected String filterClassName;
    //protected volatile transient LogFingerprintFilter messageRepeateFileter = null;//对pipline做去重
    @ENVDependence
    protected String filterMsgFieldNames;//需要去重的字段列表，用逗号分割
    @ENVDependence
    protected String filterMsgSwitch;//开启过滤的开关
    protected transient List<String> filterMsgFieldNameList;


    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            if (firstReceiveTime == null) {
                synchronized (this) {
                    if (firstReceiveTime == null) {
                        firstReceiveTime = System.currentTimeMillis();
                    }
                }
            }
            if (piplines == null) {
                return message;
            }
            double qps = 0;
            if (startQPSCount) {
                // long count= COUNT.incrementAndGet();
                long second = ((System.currentTimeMillis() - firstReceiveTime) / 1000);
                if (second == 0) {
                    second = 1;
                }
                qps = COUNT.incrementAndGet() / second;
                String name = getPipeline().getConfigureName();
                if (StringUtil.isEmpty(name)) {
                    name = piplines.get(0).getConfigureName();
                }
                double fireRate = (double)FIRE_RULE_COUNT.get() / (double)COUNT.get();
                if (!"true".equals(filterMsgSwitch)) {
                    if (COUNT.get() % 1000 == 0) {
                        System.out.println(
                            name + " qps is " + qps + "。the count is " + COUNT.get() + ".the process time is " + second
                                + "the fire rule rate is " + fireRate);
                    }
                }
            }
            boolean onlyOne = piplines.size() == 1;
            int index = 0;
            //可以启动重复过滤，把日志中的必须字段抽取出来，做去重，如果某个日志，在某个pipline执行不成功，下次类似日志过来，直接过滤掉
            String msgKey = getFilterKey(message);
            BitSetCache.BitSet bitSet = getFilterValue(msgKey);
            if(bitSet==null&&"true".equals(filterMsgSwitch)){
                bitSet=new BitSetCache.BitSet(piplines.size());
            }
            boolean needPrint = true;
            for (ChainPipeline pipline : piplines) {

                //Boolean canFilter=piplineCanFilter.get(pipline.getConfigureName());
                if (bitSet!=null&&bitSet.get(index)) {
                    index++;
                    FingerprintMetric fingerprintMetric=FingerprintCache.getInstance().getOrCreateMetric(getOrCreateFingerNameSpace());

                    double rate = fingerprintMetric.getHitCacheRate();
                    double fireRate = (double)FIRE_RULE_COUNT.get() / (double)COUNT.get();
                    if (COUNT.get() % 1000 == 0 && needPrint) {
                        needPrint = false;
                        System.out.println(
                            "qps is " + qps + ",the count is " + COUNT.get() + " the cache hit  rate " + rate
                                + " the cache size is " + fingerprintMetric.getCacheSize() + "，"
                                + "the fire rule rate is " + fireRate);
                    }
                    continue;
                }

                IMessage copyMessage = message;
                if (!onlyOne) {
                    copyMessage = message.deepCopy();
                }
                Context newContext = new Context(copyMessage);
                try {

                    pipline.doMessage(copyMessage, newContext);
                    if (!MessageGloableTrace.existFinshBranch(copyMessage)) {
                        if(bitSet!=null){
                            bitSet.set(index);
                            addNoFireMessage(msgKey,bitSet);
                        }
                    }
                    if (MessageGloableTrace.existFinshBranch(copyMessage)) {
                        FIRE_RULE_COUNT.incrementAndGet();
                    }
                } catch (Exception e) {
                    LOG.error("pipline execute error " + pipline.getConfigureName(), e);
                }
                index++;

            }
            return null;
        }

        @Override
        public String getName() {
            return SubPiplineChainStage.class.getName();
        }
    };

    @Override
    public boolean isAsyncNode() {
        for (Pipeline pipline : piplines) {
            if (pipline.isAsynNode() == true) {
                return true;
            }
        }
        return false;
    }


    /**
     * 如果确定这个message，在某个pipline不触发，则记录下来，下次直接跳过，不执行
     *
     * @param msgKey
     * @param bitSet
     */
    protected void addNoFireMessage(String msgKey, BitSetCache.BitSet bitSet) {

        if (!"true".equals(this.filterMsgSwitch)) {
            return;
        }
        FingerprintCache.getInstance().addLogFingerprint(getOrCreateFingerNameSpace(),msgKey,bitSet);
    }

    protected String getFilterKey(IMessage message) {

        if (!"true".equals(this.filterMsgSwitch)) {
            return null;
        }
        if(this.filterMsgFieldNames==null){
            return null;
        }

        return FingerprintCache.creatFingerpringKey(message,getOrCreateFingerNameSpace(), filterMsgFieldNames);
    }

    /**
     * 判读是否可以针对这条数据，过滤掉这个pipline
     *
     * @param msgKey
     * @return
     */
    protected transient String fingerNameSpace;

    protected String getOrCreateFingerNameSpace(){
        if(fingerNameSpace==null){
            fingerNameSpace= MapKeyUtil.createKey(this.getNameSpace(),this.getPipeline().getName(),getClass().getName());
        }
        return this.fingerNameSpace;
    }
    protected BitSetCache.BitSet getFilterValue(String msgKey) {
        if (!"true".equals(this.filterMsgSwitch)) {
            return null;
        }

        return FingerprintCache.getInstance().getLogFingerprint(getOrCreateFingerNameSpace(),msgKey);
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        sendSystem(message, context, piplines);
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        sendSystem(message, context, piplines);
    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        sendSystem(message, context, piplines);
    }

    /**
     * 每隔一段时间会重新刷新数据，如果有新增的pipline会加载起来，如果有删除的会去除掉
     *
     * @param configurableService
     */
    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        List<ChainPipeline> piplines = new ArrayList<>();

        loadSubPiplines(piplines, configurableService);//通过PiplineSourceJoiner装载子pipline

        //List<ChainPipeline> piplineList=configurableService.queryConfigurableByType(Pipeline.TYPE);
        //if(piplineList!=null&&piplineList.size()>0){
        //    for(ChainPipeline pipline: piplineList){
        //        if(StringUtil.isNotEmpty(piplineNameRegex)&&pipline.getConfigureName().startsWith(piplineNameRegex)&&pipline.getConfigureName().indexOf("union_1")==-1){
        //            if(pipline==(getPipline())){
        //                continue;
        //            }
        //            piplines.add(pipline);//todo 会不会被重复加载
        //
        //        }else if(piplineNames!=null&&piplineNames.contains(pipline.getConfigureName())){
        //            if(pipline==(getPipline())){
        //                continue;
        //            }
        //            piplines.add(pipline);
        //        }
        //    }
        //}

        /**
         * 做排序，确保pipline对应的index和messageRepeateFileter 的一致
         */
        Collections.sort(piplines, new Comparator<ChainPipeline>() {
            @Override
            public int compare(ChainPipeline o1, ChainPipeline o2) {
                return o1.getConfigureName().compareTo(o2.getConfigureName());
            }
        });
        if (!equalsPiplines(this.piplines, piplines)) {
            this.piplines = piplines;

        }

    }

    /**
     * 动态装配子pipline
     *
     * @param piplines
     * @param configurableService
     */
    protected void loadSubPiplines(List<ChainPipeline> piplines, IConfigurableService configurableService) {
        List<PipelineSourceJoiner> joiners = configurableService.queryConfigurableByType(PipelineSourceJoiner.TYPE);
        if (joiners == null) {
            return;
        }
        String piplineName = getPipeline().getConfigureName();
        for (PipelineSourceJoiner joiner : joiners) {
            if (piplineName.equals(joiner.getSourcePipelineName())) {
                ChainPipeline pipline = configurableService.queryConfigurable(Pipeline.TYPE, joiner.getPipelineName());
                if (pipline != null) {
                    piplines.add(pipline);
                }
            }
        }
    }

    /**
     * pipline有没有发生过变化
     *
     * @param piplines
     * @param piplines1
     * @return
     */
    private boolean equalsPiplines(List<ChainPipeline> piplines, List<ChainPipeline> piplines1) {
        if (piplines1 == null || piplines1.size() == 0) {
            return false;
        }
        if (piplines == null || piplines.size() == 0) {
            return false;
        }
        if (piplines.size() != piplines1.size()) {
            return false;
        }
        for (ChainPipeline pipline : piplines) {
            if (!piplines1.contains(pipline)) {
                return false;
            }
        }
        return true;
    }

    protected Integer getValue(String key) {
        String value = ComponentCreator.getProperties().getProperty(key);
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
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

    public String getPiplineNameRegex() {
        return piplineNameRegex;
    }

    public void setPiplineNameRegex(String piplineNameRegex) {
        this.piplineNameRegex = piplineNameRegex;
    }

    public List<String> getPiplineNames() {
        return piplineNames;
    }

    public void setPiplineNames(List<String> piplineNames) {
        this.piplineNames = piplineNames;
    }

    public List<ChainPipeline> getPiplines() {
        return piplines;
    }

    public String getFilterMsgFieldNames() {
        return filterMsgFieldNames;
    }

    public void setFilterMsgFieldNames(String filterMsgFieldNames) {
        this.filterMsgFieldNames = filterMsgFieldNames;
    }

    public String getFilterMsgSwitch() {
        return filterMsgSwitch;
    }

    public void setFilterMsgSwitch(String filterMsgSwitch) {
        this.filterMsgSwitch = filterMsgSwitch;
    }

    public String getFilterClassName() {
        return filterClassName;
    }

    public void setFilterClassName(String filterClassName) {
        this.filterClassName = filterClassName;
    }
}
