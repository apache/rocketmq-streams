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
package org.apache.rocketmq.streams.common.channel.impl.memory;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.AbstractSingleSplitSource;
import org.apache.rocketmq.streams.common.context.Message;

public class RowListSource extends AbstractSingleSplitSource {

    protected List<String> rows = new ArrayList<>();

    @Override
    protected boolean startSource() {

        for (String row : rows) {
            doReceiveMessage(createJson(row));
        }
        sendCheckpoint(getQueueId());
        executeMessage((Message) BatchFinishMessage.create());
        return true;
    }

    @Override protected void destroySource() {

    }

    public List<String> getRows() {
        return rows;
    }

    public void setRows(List<String> rows) {
        this.rows = rows;
    }
}
