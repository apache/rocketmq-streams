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

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class OutputChainStage<T extends IMessage> extends ChainStage<T> implements IAfterConfiguableRefreshListerner {
    public static final String OUT_MOCK_SWITCH = "out.mock.switch";//在配置文件中，是否打开mock的开关

    private String sinkName;

    private String metaDataName;

    /**
     * 可以关闭输出，在测试场景有效果.可以通过配置文件配置
     */
    @ENVDependence
    protected String closeOutput;

    protected transient ISink sink;

    protected transient MetaData metaData;

    /**
     * 如果需要把输出关闭或mock到一个其他channel，可以通过配置一个mockchannel。同时通过配置文件
     */
    protected transient ISink mockSink;

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            if (StringUtil.isNotEmpty(closeOutput)) {
                String tmp = closeOutput.toLowerCase();
                if ("true".equals(tmp) || "false".equals(tmp)) {
                    Boolean value = Boolean.valueOf(tmp);
                    if (value) {
                        return message;
                    }
                } else {
                    tmp = getENVVar(closeOutput);
                    if (StringUtil.isNotEmpty(tmp)) {
                        if ("true".equals(tmp.toLowerCase())) {
                            return message;
                        }
                    }
                }
            }

            /**
             * 主要是输出可能影响线上数据，可以通过配置文件的开关，把所有的输出，都指定到一个其他输出中
             */
            if(openMockChannel()){
                if(mockSink!=null){
                    mockSink.batchAdd(message);
                    return message;
                }
                return message;
            }
            sink.batchAdd(message);

            return message;
        }

        @Override
        public String getName() {
            return OutputChainStage.class.getName();
        }
    };

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        ISink realSink=null;
        if(openMockChannel()&&mockSink!=null){
            realSink=mockSink;
        }else {
            realSink=sink;
        }
        if(message.getHeader().isNeedFlush()){
            Set<String> queueIds=new HashSet<>();
            if(message.getHeader().getCheckpointQueueIds()!=null){
                queueIds.addAll(message.getHeader().getCheckpointQueueIds());
            }
            if(StringUtil.isNotEmpty(message.getHeader().getQueueId())){
                queueIds.add(message.getHeader().getQueueId());
            }
            realSink.flush(queueIds);
        }
        CheckPointState checkPointState=  new CheckPointState();
        checkPointState.setQueueIdAndOffset(realSink.getFinishedQueueIdAndOffsets(checkPointMessage));
        checkPointMessage.reply(checkPointState);

    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {

    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {

    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    protected IChannel queryChannel() {
        return configurableService.queryConfigurable(IChannel.TYPE, sinkName);
    }

    public String getSinkName() {
        return sinkName;
    }

    public void setSinkName(String sinkName) {
        this.sinkName = sinkName;
    }

    public String getMetaDataName() {
        return metaDataName;
    }

    public void setMetaDataName(String metaDataName) {
        this.metaDataName = metaDataName;
    }

    public IStageHandle getHandle() {
        return handle;
    }

    public void setHandle(IStageHandle handle) {
        this.handle = handle;
    }

    public void setSink(ISink channel) {
        this.sink = channel;
        this.setNameSpace(channel.getNameSpace());
        this.setSinkName(channel.getConfigureName());
        this.setLabel(channel.getConfigureName());
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public ISink getSink() {
        return sink;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        sink=configurableService.queryConfigurable(ISink.TYPE, sinkName);
        if(sink==null){
            sink = configurableService.queryConfigurable(IChannel.TYPE, sinkName);
        }

        metaData = configurableService.queryConfigurable(MetaData.TYPE, metaDataName);
        mockSink=getMockChannel(configurableService,sink.getNameSpace());
    }

    private ISink getMockChannel(IConfigurableService configurableService,String nameSpace) {
        String type=ComponentCreator.getProperties().getProperty("out.mock.type");
        if(type==null){
            return null;
        }
        ISink mockSink= configurableService.queryConfigurable(ISink.TYPE,OUT_MOCK_SWITCH+"_"+type);
        if(mockSink==null){
            mockSink= configurableService.queryConfigurable(IChannel.TYPE,OUT_MOCK_SWITCH+"_"+type);
        }
        return mockSink;
    }

    protected boolean openMockChannel(){
        String swtich=ComponentCreator.getProperties().getProperty(OUT_MOCK_SWITCH);
        if(swtich==null){
            return false;
        }
        if("true".equals(swtich)){
            return true;
        }
        return false;
    }

    public String getCloseOutput() {
        return closeOutput;
    }

    public void setCloseOutput(String closeOutput) {
        this.closeOutput = closeOutput;
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

}
