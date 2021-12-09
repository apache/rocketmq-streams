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

import java.util.ArrayList;
import java.util.Set;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.window.operator.impl.ShuffleOverWindow;

public class OverWindowStream {
    protected ShuffleOverWindow window;

    /**
     * 创建datastream时使用
     */
    protected PipelineBuilder pipelineBuilder;
    protected Set<PipelineBuilder> otherPipelineBuilders;
    protected ChainStage<?> currentChainStage;

    public OverWindowStream(ShuffleOverWindow window, PipelineBuilder pipelineBuilder, Set<PipelineBuilder> pipelineBuilders, ChainStage<?> currentChainStage) {
        this.pipelineBuilder = pipelineBuilder;
        this.otherPipelineBuilders = pipelineBuilders;
        this.currentChainStage = currentChainStage;
        this.window = window;
    }


    public OverWindowStream setRowNumName(String name){
        window.setRowNumerName(name);
        return this;
    }

    public OverWindowStream addOrderByFieldName(String fieldName,boolean isAsc){
        if(window.getOrderFieldNames()==null){
            window.setOrderFieldNames(new ArrayList<>());
        }
        window.getOrderFieldNames().add(fieldName+";"+(isAsc?"true":"false"));
        return this;
    }

    public OverWindowStream setTopN(int topN){
        window.setTopN(topN);
        return this;
    }


    /**
     * 以哪几个字段做分组，支持多个字段
     *
     * @param fieldNames
     * @return
     */
    public OverWindowStream groupBy(String... fieldNames) {
        window.setGroupByFieldName(MapKeyUtil.createKeyBySign(";", fieldNames));
        for (String fieldName : fieldNames) {
            window.getSelectMap().put(fieldName, fieldName);
        }

        return this;
    }

    public DataStream toDataSteam() {
        return new DataStream(pipelineBuilder, otherPipelineBuilders, currentChainStage);
    }


}
