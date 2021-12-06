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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;

public abstract class AbstractUDFSink extends AbstractSink{
    @Override protected boolean batchInsert(List<IMessage> messages) {
        return batchInsert(messages,this);
    }
     public static boolean batchInsert(List<IMessage> messages,AbstractUDFSink sink) {

        if (messages == null) {
            return true;
        }
        try {
            Map<String, List<IMessage>> msgsByQueueId = new HashMap<>();// group by queueId, if the message not contains queue info ,the set default string as default queueId
            Map<String, ISplit> messageQueueMap = new HashMap<>();//if has queue id in message, save the map for queueid 2 messagequeeue
            String defaultQueueId = "<null>";//message is not contains queue ,use default
            for (IMessage msg : messages) {
                ISplit channelQueue = sink.getSplit(msg);
                String queueId = defaultQueueId;
                if (channelQueue != null) {
                    queueId = channelQueue.getQueueId();
                    messageQueueMap.put(queueId, channelQueue);
                }
                List<IMessage> messageList = msgsByQueueId.get(queueId);
                if (messageList == null) {
                    messageList = new ArrayList<>();
                    msgsByQueueId.put(queueId, messageList);
                }
                messageList.add(msg);
            }
            List<IMessage> messageList = msgsByQueueId.get(defaultQueueId);
            if (messageList != null) {
                sink.sendMessage2Store(messageList);
                messageQueueMap.remove(defaultQueueId);
            }
            if (messageQueueMap.size() <= 0) {
                return true;
            }
            for (String queueId : msgsByQueueId.keySet()) {
                messageList = msgsByQueueId.get(queueId);
                ISplit split=messageQueueMap.get(queueId);
                sink.sendMessage2Store(split,messageList);

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("batch insert error ", e);
        }

        return true;
    }

    protected abstract void sendMessage2Store(List<IMessage> messageList);

    protected abstract void sendMessage2Store(ISplit split,List<IMessage> messageList);
}
