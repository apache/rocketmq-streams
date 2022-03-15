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
package org.apache.rocketmq.streams.common.channel.sink;

import java.util.List;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;

public abstract class AbstractSupportShuffleUDFSink extends AbstractSupportShuffleSink {

    @Override protected boolean batchInsert(List<IMessage> messages) {
        AbstractSupportShuffleUDFSink sink = this;
        return AbstractUDFSink.batchInsert(messages, new AbstractUDFSink() {
            @Override protected void sendMessage2Store(List<IMessage> messageList) {
                sink.sendMessage2Store(messageList);
            }

            @Override protected void sendMessage2Store(ISplit<?, ?> split, List<IMessage> messageList) {
                sink.sendMessage2Store(split, messageList);
            }
        });
    }

    @Override
    public int getSplitNum() {
        List<ISplit<?, ?>> splits = getSplitList();
        if (splits == null || splits.size() == 0) {
            return 0;
        }
        return splits.size();
    }

    protected abstract void sendMessage2Store(List<IMessage> messageList);

    protected abstract void sendMessage2Store(ISplit<?, ?> split, List<IMessage> messageList);
}
