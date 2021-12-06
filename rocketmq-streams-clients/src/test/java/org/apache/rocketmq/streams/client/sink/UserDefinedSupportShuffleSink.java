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
package org.apache.rocketmq.streams.client.sink;

import java.util.List;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleUDFSink;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;

public class UserDefinedSupportShuffleSink extends AbstractSupportShuffleUDFSink {
    @Override public String getShuffleTopicFieldName() {
        return null;
    }

    @Override protected void createTopicIfNotExist(int splitNum) {

    }

    @Override public List<ISplit> getSplitList() {
        return null;
    }

    @Override protected void sendMessage2Store(List<IMessage> messageList) {

    }

    @Override protected void sendMessage2Store(ISplit split, List<IMessage> messageList) {

    }
}
