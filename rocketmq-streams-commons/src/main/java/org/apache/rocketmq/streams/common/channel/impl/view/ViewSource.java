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

import java.util.List;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;

public class ViewSource extends AbstractSource {
    protected String tableName;
    protected AbstractSource rootSource;

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
    }

    @Override protected boolean startSource() {
        return true;
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        return rootSource.fetchAllSplits();
    }

    @Override protected void destroySource() {

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public AbstractSource getRootSource() {
        return rootSource;
    }

    public void setRootSource(AbstractSource rootSource) {
        this.rootSource = rootSource;
    }
}
