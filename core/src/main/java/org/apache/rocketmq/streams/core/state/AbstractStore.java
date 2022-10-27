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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.util.Utils;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public abstract class AbstractStore {

    protected MessageQueue convertSourceTopicQueue2StateTopicQueue(MessageQueue messageQueue) {
        HashSet<MessageQueue> messageQueues = new HashSet<>();
        messageQueues.add(messageQueue);

        Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(messageQueues);

        Iterator<MessageQueue> iterator = stateTopicQueue.iterator();
        return iterator.next();
    }
    protected Set<MessageQueue> convertSourceTopicQueue2StateTopicQueue(Set<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.size() == 0) {
            return new HashSet<>();
        }

        HashSet<MessageQueue> result = new HashSet<>();
        for (MessageQueue messageQueue : messageQueues) {
            if (messageQueue.getTopic().endsWith(Constant.STATE_TOPIC_SUFFIX)) {
                result.add(messageQueue);
                continue;
            }
            MessageQueue queue = new MessageQueue(messageQueue.getTopic() + Constant.STATE_TOPIC_SUFFIX, messageQueue.getBrokerName(), messageQueue.getQueueId());
            result.add(queue);
        }

        return result;
    }

    protected static String stateTopic2SourceTopic(String stateTopic) {
        if (StringUtils.isEmpty(stateTopic)) {
            return null;
        }

        return stateTopic.substring(0, stateTopic.lastIndexOf(Constant.STATE_TOPIC_SUFFIX));
    }


    protected String buildKey(MessageExt messageExt) {
        return Utils.buildKey(messageExt.getBrokerName(), messageExt.getTopic(), messageExt.getQueueId());
    }

    protected String buildKey(MessageQueue messageQueue) {
        return Utils.buildKey(messageQueue.getBrokerName(), messageQueue.getTopic(), messageQueue.getQueueId());
    }

}
