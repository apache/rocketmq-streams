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

package org.apache.rocketmq.streams.common.channel.impl.view;

import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.task.TaskAssigner;

public class ViewSource extends AbstractSource {
    protected String tableName;


    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        TaskAssigner taskAssigner=new TaskAssigner();
        taskAssigner.setTaskName(tableName);
        taskAssigner.setPipelineName(pipelineBuilder.getPipelineName());;
        pipelineBuilder.addConfigurables(taskAssigner);
        pipelineBuilder.addConfigurables(this);
    }



    @Override protected boolean startSource() {
        return true;
    }

    @Override public boolean supportNewSplitFind() {
        return false;
    }

    @Override public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override public boolean supportOffsetRest() {
        return false;
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
