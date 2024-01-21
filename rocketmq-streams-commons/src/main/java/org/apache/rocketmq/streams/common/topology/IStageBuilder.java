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
package org.apache.rocketmq.streams.common.topology;

import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;

/**
 * 通过它构建pipeline中的chain stage。有各个子类实现
 */
public interface IStageBuilder<T> {

    /**
     * 返回构建pipeline 的stage对象。也包含了channel
     *
     * @param pipelineBuilder
     * @return
     */
    T createStageChain(PipelineBuilder pipelineBuilder);

    /**
     * 构建过程中产生的configurable
     *
     * @param pipelineBuilder
     * @return
     */
    void addConfigurables(PipelineBuilder pipelineBuilder);
}
