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
package org.apache.rocketmq.streams.core.util;


import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class Utils {
    public static final String pattern = "%s@%s@%s";
    public static String buildKey(String brokerName, String topic, int queueId) {
        return String.format(pattern, brokerName, topic, queueId);
    }


//    public static MessageQueue convertSourceTopicQueue2StateTopicQueue(MessageQueue messageQueue) {
//        HashSet<MessageQueue> messageQueues = new HashSet<>();
//        messageQueues.add(messageQueue);
//
//        Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(messageQueues);
//
//        Iterator<MessageQueue> iterator = stateTopicQueue.iterator();
//        return iterator.next();
//    }
//    public static Set<MessageQueue> convertSourceTopicQueue2StateTopicQueue(Set<MessageQueue> messageQueues) {
//        if (messageQueues == null || messageQueues.size() == 0) {
//            return new HashSet<>();
//        }
//
//        HashSet<MessageQueue> result = new HashSet<>();
//        for (MessageQueue messageQueue : messageQueues) {
//            if (messageQueue.getTopic().endsWith(Constant.STATE_TOPIC_SUFFIX)) {
//                continue;
//            }
//            MessageQueue queue = new MessageQueue(messageQueue.getTopic() + Constant.STATE_TOPIC_SUFFIX, messageQueue.getBrokerName(), messageQueue.getQueueId());
//            result.add(queue);
//        }
//
//        return result;
//    }
}
