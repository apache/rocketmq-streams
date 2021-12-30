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
package org.apache.rocketmq.streams.common.channel.impl.mutiltask;

import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;

public class MutilTaskSink extends AbstractSink implements IAfterConfigurableRefreshListener {

    protected transient StreamsTask streamsTask;
    protected String taskName;

    @Override public boolean batchAdd(IMessage message) {
        Context context = new Context(message);
        streamsTask.doMessage(message, context);
        return true;
    }

    @Override public boolean batchSave(List<IMessage> messages) {
        if (messages != null) {
            for (IMessage message : messages) {
                batchAdd(message);
            }
        }
        return true;
    }

    @Override public boolean flush(Set<String> splitIds) {
        return true;
    }

    @Override public boolean flush(String... splitIds) {
        return true;
    }

    @Override protected boolean batchInsert(List<IMessage> messages) {
        throw new RuntimeException("can not support this method");
    }

    @Override public void closeAutoFlush() {

    }

    @Override public boolean flush() {
        return true;
    }

    @Override protected boolean initConfigurable() {
        return true;
    }

    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        streamsTask = new StreamsTask();
        streamsTask.setConfigureName(taskName);
        streamsTask.setNameSpace(getNameSpace());
        streamsTask.init();
        streamsTask.doProcessAfterRefreshConfigurable(configurableService);
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }
}
