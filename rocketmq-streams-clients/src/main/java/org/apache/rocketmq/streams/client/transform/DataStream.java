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

package org.apache.rocketmq.streams.client.transform;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.client.DataStreamAction;
import org.apache.rocketmq.streams.client.transform.window.WindowInfo;
import org.apache.rocketmq.streams.common.channel.impl.OutputPrintChannel;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.apache.rocketmq.streams.common.functions.*;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.Union;
import org.apache.rocketmq.streams.common.topology.stages.udf.StageBuilder;
import org.apache.rocketmq.streams.common.topology.stages.udf.UDFUnionChainStage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.db.sink.DBSink;
import org.apache.rocketmq.streams.dim.model.DBDim;
import org.apache.rocketmq.streams.filter.operator.FilterOperator;
import org.apache.rocketmq.streams.script.operator.impl.ScriptOperator;
import org.apache.rocketmq.streams.sink.RocketMQSink;
import org.apache.rocketmq.streams.window.builder.WindowBuilder;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.join.JoinWindow;

import java.io.Serializable;
import java.util.Set;

public class DataStream implements Serializable {

    protected PipelineBuilder mainPipelineBuilder;
    protected Set<PipelineBuilder> otherPipelineBuilders;
    protected ChainStage<?> currentChainStage;

    public DataStream(String namespace, String pipelineName) {
        this.mainPipelineBuilder = new PipelineBuilder(namespace, pipelineName);
        this.otherPipelineBuilders = Sets.newHashSet();
    }

    public DataStream(PipelineBuilder pipelineBuilder, ChainStage<?> currentChainStage) {
        this.mainPipelineBuilder = pipelineBuilder;
        this.currentChainStage = currentChainStage;
    }

    public DataStream(PipelineBuilder pipelineBuilder, Set<PipelineBuilder> pipelineBuilders, ChainStage<?> currentChainStage) {
        this.mainPipelineBuilder = pipelineBuilder;
        this.otherPipelineBuilders = pipelineBuilders;
        this.currentChainStage = currentChainStage;
    }

    public DataStream script(String script) {
        ChainStage<?> stage = this.mainPipelineBuilder.createStage(new ScriptOperator(script));
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);

        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    public DataStream filter(String expressions) {
        ChainStage<?> stage = this.mainPipelineBuilder.createStage(new FilterOperator(expressions));
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    public <T, O> DataStream map(MapFunction<T, O> mapFunction) {
        StageBuilder stageBuilder = new StageBuilder() {
            @Override
            protected <T> T operate(IMessage message, AbstractContext context) {
                try {
                    O o = (O)(message.getMessageValue());
                    T result = (T)mapFunction.map(o);
                    if (result != message.getMessageValue()) {
                        if (result instanceof JSONObject) {
                            message.setMessageBody((JSONObject)result);
                        } else {
                            message.setMessageBody(new UserDefinedMessage(result));
                        }
                    }
                    return null;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        ChainStage<?> stage = this.mainPipelineBuilder.createStage(stageBuilder);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }


    public <T, O> DataStream flatMap(FlatMapFunction<T, O> mapFunction) {
        StageBuilder stageBuilder = new StageBuilder() {
            @Override
            protected <T> T operate(IMessage message, AbstractContext context) {
                try {
                    O o = (O)(message.getMessageValue());
                    List<T> result =(List<T>)mapFunction.flatMap(o);
                    if(result==null||result.size()==0){
                        context.breakExecute();
                    }
                    List<IMessage> splitMessages=new ArrayList<>();
                    for(T t:result){
                        Message subMessage=null;
                        if (result instanceof JSONObject) {
                            subMessage=new Message((JSONObject)t);
                        } else {
                            subMessage=new Message(new UserDefinedMessage(result));
                        }
                        splitMessages.add(subMessage);
                    }
                    context.openSplitModel();;
                    context.setSplitMessages(splitMessages);
                    return null;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        ChainStage<?> stage = this.mainPipelineBuilder.createStage(stageBuilder);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    public <O> DataStream filter(final FilterFunction<O> filterFunction) {
        StageBuilder mapUDFOperator = new StageBuilder() {

            @Override
            protected <T> T operate(IMessage message, AbstractContext context) {
                try {
                    boolean isFilter = filterFunction.filter((O)message.getMessageValue());
                    if (isFilter) {
                        context.breakExecute();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
        ChainStage stage = this.mainPipelineBuilder.createStage(mapUDFOperator);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    /**
     * windows streams
     *
     * @param windowInfo 通过不同窗口类型动of方法创建，SessionWindow.of(Time.seconds(10))
     * @return WindowStream
     */
    public WindowStream window(WindowInfo windowInfo) {
        AbstractWindow window = windowInfo.createWindow();
        ChainStage<?> stage = this.mainPipelineBuilder.createStage(window);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new WindowStream(window, this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    /**
     * 通用增加stage的方法，低级接口，适合用户自定义stage的场景
     *
     * @param stageBuilder 创建stage和变量的接口
     * @return DataStream
     */
    public DataStream addStage(IStageBuilder stageBuilder) {
        ChainStage<?> stage = this.mainPipelineBuilder.createStage(stageBuilder);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    /**
     * 创建join stream。实现原理，通过共享一个JoinWindow，并打标左右流，在join windown完成缓存，join逻辑
     *
     * @param rightStream 通过不同窗口类型动of方法创建，SessionWindow.of(Time.seconds(10))
     * @return
     */
    public JoinStream join(DataStream rightStream) {
        JoinWindow window = WindowBuilder.createDefaultJoinWindow();
        //处理左边分支，增加map，主要是增加msg msgRouteFromLable->增加窗口stage
        ChainStage<?> leftScriptStage = this.mainPipelineBuilder.createStage(new ScriptOperator("setHeader(msgRouteFromLable,'" + MessageHeader.JOIN_LEFT + "')"));
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, leftScriptStage);
        this.currentChainStage = leftScriptStage;
        ChainStage<?> leftWindowStage = this.mainPipelineBuilder.createStage(window);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, leftWindowStage);

        //处理右流，右流增加script
        DataStream dataStream = rightStream.script("setHeader(msgRouteFromLable,'" + MessageHeader.JOIN_RIGHT + "')").addStage(window);
        //dataStream.addStage(window);

        addOtherDataStream(rightStream);
        return new JoinStream(window, this.mainPipelineBuilder, this.otherPipelineBuilders, leftWindowStage);
    }

    /**
     * 通过共享对象union，完成两个数据汇聚，左流需要设置isMainStream=true
     *
     * @param rightStream
     * @return
     */
    public DataStream union(DataStream rightStream) {
        Union union = new Union();

        //处理左流，做流的isMain设置成true
        UDFUnionChainStage chainStage = (UDFUnionChainStage)this.mainPipelineBuilder.createStage(union);
        chainStage.setMainStream(true);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, chainStage);

        //处理右流，做流的isMain设置成true
        rightStream.addStage(union);

        addOtherDataStream(rightStream);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, chainStage);
    }

    /**
     * 把一个流拆分成多个流，通过设置不同流的标签实现
     *
     * @param splitFunction 拆分流的具体逻辑
     * @return
     */
    public SplitStream split(SplitFunction splitFunction) {
        StageBuilder operator = new StageBuilder() {
            @Override
            protected <T> T operate(IMessage message, AbstractContext context) {
                String labelName = splitFunction.split(message.getMessageValue());
                message.getHeader().addRouteLable(labelName);
                return null;
            }
        };
        ChainStage<?> stage = this.mainPipelineBuilder.createStage(operator);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new SplitStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    /**
     * 维表join,mysql场景，不需要指定jdbcdriver
     *
     * @param url
     * @param userName
     * @param password
     * @param sqlOrTableName
     * @return
     */
    public JoinStream join(String url, String userName, String password, String sqlOrTableName, long pollingTimeMintue) {
        return join(url, userName, password, sqlOrTableName, null, pollingTimeMintue);
    }

    /**
     * 维表join
     *
     * @param url
     * @param userName
     * @param password
     * @param sqlOrTableName
     * @return
     */
    public JoinStream join(String url, String userName, String password, String sqlOrTableName, String jdbcDriver, long pollingTimeMinute) {
        DBDim dbDim = new DBDim();
        dbDim.setUrl(url);
        dbDim.setUserName(userName);
        dbDim.setPassword(password);
        dbDim.setSql(sqlOrTableName);
        dbDim.setPollingTimeMintue(pollingTimeMinute);
        dbDim.setJdbcdriver(jdbcDriver);
        this.mainPipelineBuilder.addConfigurables(dbDim);
        return new JoinStream(dbDim, mainPipelineBuilder, otherPipelineBuilders, currentChainStage);
    }

    /**
     * 遍历所有数据
     *
     * @param forEachFunction
     * @param <O>
     * @return
     */
    public <O> DataStream forEach(ForEachFunction<O> forEachFunction) {
        StageBuilder selfChainStage = new StageBuilder() {
            @Override
            protected <T> T operate(IMessage message, AbstractContext context) {
                forEachFunction.foreach((O)message.getMessageValue());
                return null;
            }
        };
        ChainStage stage = this.mainPipelineBuilder.createStage(selfChainStage);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    /**
     * 遍历所有数据
     *
     * @param forEachFunction
     * @param <O>
     * @return
     */
    public <O> DataStream forEachMessage(ForEachMessageFunction forEachFunction) {
        StageBuilder selfChainStage = new StageBuilder() {
            @Override
            protected <T> T operate(IMessage message, AbstractContext context) {
                forEachFunction.foreach(message, context);
                return null;
            }
        };
        ChainStage stage = this.mainPipelineBuilder.createStage(selfChainStage);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    /**
     * 只保留需要的字段
     *
     * @param fieldNames
     */
    public DataStream selectFields(String... fieldNames) {
        ChainStage stage = this.mainPipelineBuilder.createStage(new ScriptOperator("retain(" + MapKeyUtil.createKeyBySign(",", fieldNames) + ")"));
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(this.mainPipelineBuilder, this.otherPipelineBuilders, stage);
    }

    /**
     * 启动流任务
     */
    public void start() {
        start(false);
    }

    /**
     * 启动流任务
     */
    public void asynStart() {
        start(true);
    }

    protected void start(boolean isAsyn) {
        if (this.mainPipelineBuilder == null) {
            return;
        }


        ConfigurableComponent configurableComponent = ComponentCreator.getComponent(mainPipelineBuilder.getPipelineNameSpace(), ConfigurableComponent.class, ConfigureFileKey.CONNECT_TYPE + ":memory");
        ChainPipeline pipeline = this.mainPipelineBuilder.build(configurableComponent.getService());
        pipeline.startChannel();
        if (this.otherPipelineBuilders != null) {
            for (PipelineBuilder builder : otherPipelineBuilders) {
                ChainPipeline otherPipeline = builder.build(configurableComponent.getService());
                otherPipeline.startChannel();
            }
        }
        if (isAsyn) {
            return;
        }
        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 把其他流的 pipelinebuilder 放到set中
     *
     * @param rightSource
     */
    protected void addOtherDataStream(DataStream rightSource) {
        //如果是多流join，需要把把piplinebuider保存下来，在启动时，启动多个pipline
        if (!rightSource.mainPipelineBuilder.equals(this.mainPipelineBuilder)) {
            this.otherPipelineBuilders.add(rightSource.mainPipelineBuilder);
        }

        this.otherPipelineBuilders.addAll(rightSource.otherPipelineBuilders);
    }

    public DataStreamAction toFile(String filePath) {
        FileSink fileChannel = new FileSink(filePath);
        ChainStage<?> output = mainPipelineBuilder.createStage(fileChannel);
        mainPipelineBuilder.setTopologyStages(currentChainStage, output);
        return new DataStreamAction(this.mainPipelineBuilder, this.otherPipelineBuilders, output);
    }

    public DataStreamAction toPrint() {
        return toPrint(-1);
    }

    public DataStreamAction toPrint(int batchSize) {
        OutputPrintChannel outputPrintChannel = new OutputPrintChannel();
        if (batchSize > 0) {
            outputPrintChannel.setBatchSize(batchSize);
        }
        ChainStage output = this.mainPipelineBuilder.createStage(outputPrintChannel);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, output);
        return new DataStreamAction(this.mainPipelineBuilder, this.otherPipelineBuilders, output);
    }

    public DataStreamAction toDB(String url, String userName, String password, String tableName) {
        DBSink dbChannel = new DBSink(url, userName, password);
        dbChannel.setTableName(tableName);
        ChainStage<?> output = this.mainPipelineBuilder.createStage(dbChannel);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, output);
        return new DataStreamAction(this.mainPipelineBuilder, this.otherPipelineBuilders, output);
    }



    public DataStreamAction toRocketmq(String topic, String groupName, String endpoint, String namesrvAddr,
                                       String accessKey, String secretKey, String instanceId) {
        return toRocketmq(topic, "*", groupName, namesrvAddr, endpoint, accessKey, secretKey, instanceId);
    }

    public DataStreamAction toRocketmq(String topic, String tags, String groupName, String endpoint,
                                       String namesrvAddr, String accessKey, String secretKey, String instanceId) {
        return toRocketmq(topic, tags, -1, groupName, namesrvAddr, endpoint, accessKey, secretKey, instanceId);
    }

    public DataStreamAction toRocketmq(String topic, String tags, int batchSize, String groupName,
                                       String endpoint, String namesrvAddr, String accessKey, String secretKey, String instanceId) {
        RocketMQSink rocketMQSink = new RocketMQSink();
        rocketMQSink.setTopic(topic);
        rocketMQSink.setTags(tags);
        rocketMQSink.setGroupName(groupName);
        rocketMQSink.setNamesrvAddr(namesrvAddr);
        if (batchSize > 0) {
            rocketMQSink.setBatchSize(batchSize);
        }
        ChainStage<?> output = this.mainPipelineBuilder.createStage(rocketMQSink);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, output);
        return new DataStreamAction(this.mainPipelineBuilder, this.otherPipelineBuilders, output);
    }

    public DataStreamAction to(ISink<?> sink) {
        ChainStage<?> output = this.mainPipelineBuilder.createStage(sink);
        this.mainPipelineBuilder.setTopologyStages(currentChainStage, output);
        return new DataStreamAction(this.mainPipelineBuilder, this.otherPipelineBuilders, output);
    }
}
