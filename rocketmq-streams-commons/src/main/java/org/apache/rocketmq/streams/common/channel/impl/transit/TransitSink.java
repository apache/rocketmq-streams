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

package org.apache.rocketmq.streams.common.channel.impl.transit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.LogFingerprintFilter;
import org.apache.rocketmq.streams.common.optimization.MessageGloableTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.model.PipelineSourceJoiner;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

public class TransitSink extends AbstractSink  implements IAfterConfigurableRefreshListener {
    private static final Log LOG = LogFactory.getLog(TransitSink.class);
    protected transient List<ChainPipeline> piplines = new ArrayList<>();
    protected String tableName;
    @ENVDependence
    protected String logFingerprintFieldNames;//config log finger

    @Override public boolean batchAdd(IMessage message) {
        boolean onlyOne = piplines.size() == 1;
        int index = 0;
        //可以启动重复过滤，把日志中的必须字段抽取出来，做去重，如果某个日志，在某个pipline执行不成功，下次类似日志过来，直接过滤掉
        BitSetCache.BitSet bitSet = getFilterValue(message);
        if(bitSet==null&&logFingerprintFieldNames!=null){
            bitSet=new BitSetCache.BitSet(piplines.size());
        }
        for (ChainPipeline pipline : piplines) {
            if (bitSet!=null&&bitSet.get(index)) {
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
                        addNoFireMessage(message, bitSet);
                    }

                }

            } catch (Exception e) {
                LOG.error("pipline execute error " + pipline.getConfigureName(), e);
            }
            index++;

        }
        return true;
    }



    @Override public boolean checkpoint(Set<String> splitIds) {
        return super.checkpoint(splitIds);
    }

    @Override public boolean checkpoint(String... splitIds) {
        return super.checkpoint(splitIds);
    }

    @Override protected boolean batchInsert(List<IMessage> messages) {
        return false;
    }

    @Override protected boolean initConfigurable() {
        boolean success= super.initConfigurable();
        this.messageCache=null;
        return true;
    }

    @Override public boolean flush(Set<String> splitIds) {
        return true;
    }

    @Override public boolean flush(String... splitIds) {
        return true;
    }

    @Override public boolean flush() {
        return true;
    }

    @Override public void openAutoFlush() {
    }

    @Override public void closeAutoFlush() {
    }

    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        List<ChainPipeline> piplines = new ArrayList<>();

        loadSubPiplines(piplines, configurableService);//通过PiplineSourceJoiner装载子pipline


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
     * 如果确定这个message，在某个pipline不触发，则记录下来，下次直接跳过，不执行
     *
     * @param message
     * @param message
     */
    protected void addNoFireMessage(IMessage message, BitSetCache.BitSet bitSet) {
        if (this.logFingerprintFieldNames == null) {
            return;
        }

        FingerprintCache.getInstance().addLogFingerprint(getOrCreateFingerNameSpace(),message,bitSet,this.logFingerprintFieldNames);
    }


    /**
     * 判读是否可以针对这条数据，过滤掉这个pipline
     *
     * @param message
     * @return
     */
    protected BitSetCache.BitSet getFilterValue(IMessage message) {
        if (this.logFingerprintFieldNames == null) {
            return null;
        }

        return FingerprintCache.getInstance().getLogFingerprint(getOrCreateFingerNameSpace(),message,this.logFingerprintFieldNames);
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
            fingerNameSpace= MapKeyUtil.createKey(this.getNameSpace(),this.getConfigureName());
        }
        return this.fingerNameSpace;
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
        for (PipelineSourceJoiner joiner : joiners) {
            if (tableName.equals(joiner.getSourcePipelineName())) {
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

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getLogFingerprintFieldNames() {
        return logFingerprintFieldNames;
    }

    public void setLogFingerprintFieldNames(String logFingerprintFieldNames) {
        this.logFingerprintFieldNames = logFingerprintFieldNames;
    }
}
