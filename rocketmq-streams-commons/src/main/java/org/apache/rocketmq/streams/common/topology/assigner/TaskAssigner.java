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
package org.apache.rocketmq.streams.common.topology.assigner;

import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

/**
 * 帮助某个pipeline 关联对应的数据源pipeline。 主要用于多个同源任务自动装配的场景 namespace:source pipeline的name space name：pipelineName
 */
public class TaskAssigner extends BasedConfigurable {
    public static String TYPE = "assigner";
    protected String taskName;//数据源对应的pipline name
    protected String pipelineName;//需要关联的pipline name
    protected Integer weight = 1;

    public TaskAssigner() {
        setType(TYPE);
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}
