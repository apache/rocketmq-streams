package org.apache.rocketmq.streams.core.state;
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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.util.Utils;
import java.util.HashSet;
import java.util.Set;

public abstract class AbstractStore implements StateStore {
    enum StoreState {
        UNINITIALIZED,
        INITIALIZED,
        LOADING,
        LOADING_FINISHED
    }

    public final static String STATE_TOPIC_SUFFIX = "-stateTopic";

    protected StoreState state = StoreState.UNINITIALIZED;
    protected final Object lock = new Object();

    @Override
    public void recover(Set<MessageQueue> addQueues, Set<MessageQueue> removeQueues) throws Throwable {
    }

    protected Set<MessageQueue> convertSourceTopicQueue2StateTopicQueue(Set<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.size() == 0) {
            return new HashSet<>();
        }

        HashSet<MessageQueue> result = new HashSet<>();
        for (MessageQueue messageQueue : messageQueues) {
            if (messageQueue.getTopic().endsWith(STATE_TOPIC_SUFFIX)) {
                continue;
            }
            MessageQueue queue = new MessageQueue(messageQueue.getTopic() + STATE_TOPIC_SUFFIX, messageQueue.getBrokerName(), messageQueue.getQueueId());
            result.add(queue);
        }

        return result;
    }

    protected String buildKey(MessageExt messageExt) {
        return Utils.buildKey(messageExt.getBrokerName(), messageExt.getTopic(), messageExt.getQueueId());
    }

    protected String buildKey(MessageQueue messageQueue) {
        return Utils.buildKey(messageQueue.getBrokerName(), messageQueue.getTopic(), messageQueue.getQueueId());
    }

    protected byte[] object2Byte(Object target) {
        if (target == null) {
            return new byte[]{};
        }

        return JSON.toJSONBytes(target, SerializerFeature.WriteClassName);
    }

    protected Object byte2Object(byte[] source) {
        return JSON.parse(source);
    }

}
