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

package org.apache.rocketmq.streams.client;

import com.google.common.collect.Maps;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.rocketmq.streams.client.strategy.LogFingerprintStrategy;
import org.apache.rocketmq.streams.client.strategy.Strategy;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.optimization.LogFingerprintFilter;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;

public class DataStreamAction extends DataStream {

    private final Map<String, Object> properties = Maps.newHashMap();

    public DataStreamAction(String namespace, String pipelineName) {
        super(namespace, pipelineName);
    }

    public DataStreamAction(PipelineBuilder pipelineBuilder, Set<PipelineBuilder> pipelineBuilders, ChainStage<?> currentChainStage) {
        super(pipelineBuilder, pipelineBuilders, currentChainStage);
    }

    public DataStreamAction with(Strategy... strategies) {
        Properties properties = new Properties();
        for (Strategy strategy : strategies) {

            if(strategy instanceof LogFingerprintStrategy){
                ISource source=this.mainPipelineBuilder.getPipeline().getSource();
                if(source instanceof AbstractSource){
                    AbstractSource abstractSource=(AbstractSource)source;
                    String[] logFingerprintFields=((LogFingerprintStrategy)strategy).getLogFingerprintFields();
                    if(logFingerprintFields!=null){
                        List<String> logFingerprintFieldList=new ArrayList<>();
                        for(String loglogFingerprintField:logFingerprintFields){
                            logFingerprintFieldList.add(loglogFingerprintField);
                        }
                        abstractSource.setLogFingerprintFields(logFingerprintFieldList);
                    }
                }
            }
            properties.putAll(strategy.getStrategyProperties());
        }
        ComponentCreator.createProperties(properties);
        return this;
    }

    /**
     * 启动流任务
     */
    @Override
    public void start() {
        start(false);
    }

    /**
     * 启动流任务
     */
    public void asyncStart() {
        start(true);
    }

    @Override
    protected void start(boolean isAsync) {
        if (this.mainPipelineBuilder == null) {
            return;
        }
        properties.put(ConfigureFileKey.CONNECT_TYPE, "memory");
        String[] kvs = new String[properties.size()];
        int i = 0;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            kvs[i++] = entry.getKey() + ":" + entry.getValue();
        }

        ConfigurableComponent configurableComponent = ComponentCreator.getComponent(mainPipelineBuilder.getPipelineNameSpace(), ConfigurableComponent.class, kvs);
        ChainPipeline pipeline = this.mainPipelineBuilder.build(configurableComponent.getService());
        pipeline.startChannel();
        if (this.otherPipelineBuilders != null) {
            for (PipelineBuilder builder : otherPipelineBuilders) {
                ChainPipeline otherPipeline = builder.build(configurableComponent.getService());
                otherPipeline.startChannel();
            }
        }
        if (isAsync) {
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

}
