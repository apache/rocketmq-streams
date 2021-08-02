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
package org.apache.rocketmq.streams.common.topology.model;

import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

/**
 * 帮助某个pipline 关联对应的数据源pipline。 主要用于多个同源任务自动装配的场景 namespace:source pipline的name space name：piplineName
 */
public class PipelineSourceJoiner extends BasedConfigurable {
    public static String TYPE = "joiner";
    protected String sourcePipelineName;//数据源对应的pipline name
    protected String pipelineName;//需要关联的pipline name

    public PipelineSourceJoiner() {
        setType(TYPE);
    }

    public String getSourcePipelineName() {
        return sourcePipelineName;
    }

    public void setSourcePipelineName(String sourcePipelineName) {
        this.sourcePipelineName = sourcePipelineName;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }
}
