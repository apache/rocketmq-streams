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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.streams.common.utils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

public class RocketmqUtil {

    public static DefaultMQProducer initDefaultMQProducer(String namesrvAddr, String instanceName, String produceGroup) {
        return initDefaultMQProducer(false, null, null, namesrvAddr, instanceName, produceGroup, null, null);
    }

    public static DefaultMQProducer initDefaultMQProducer(boolean aclEnable, String accessKey, String secretKey, String namesrvAddr, String instanceName, String produceGroup, Integer msgTimeout, Integer maxMessageSize) {
        RPCHook rpcHook = null;
        if (aclEnable) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        DefaultMQProducer producer = new DefaultMQProducer(rpcHook);
        producer.setNamesrvAddr(namesrvAddr);
        producer.setInstanceName(instanceName);
        if (produceGroup != null) {
            producer.setProducerGroup(produceGroup);
        }
        if (msgTimeout != null) {
            producer.setSendMsgTimeout(msgTimeout);
        }
        if (maxMessageSize != null) {
            producer.setMaxMessageSize(maxMessageSize);
        }
        producer.setLanguage(LanguageCode.JAVA);
        producer.setEnableBackpressureForAsyncMode(true);
        return producer;
    }

    public static DefaultLitePullConsumer initDefaultMQPullConsumer(String namesrvAddr, String instanceName, String consumerGroup, String topic) {
        return initDefaultMQPullConsumer(namesrvAddr, instanceName, consumerGroup, topic, null);
    }

    public static DefaultLitePullConsumer initDefaultMQPullConsumer(String namesrvAddr, String instanceName, String consumerGroup, String topic, ConsumeFromWhere consumeFromWhere) {
        return initDefaultMQPullConsumer(false, null, null, namesrvAddr, instanceName, consumerGroup, topic, consumeFromWhere, null);
    }

    public static DefaultLitePullConsumer initDefaultMQPullConsumer(boolean aclEnable, String accessKey, String secretKey, String namesrvAddr, String instanceName, String consumerGroup, String topic, ConsumeFromWhere consumeFromWhere,
        Long messageConsumeTimeout) {
        RPCHook rpcHook = null;
        if (aclEnable) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        try {
            DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(consumerGroup, rpcHook);
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setInstanceName(instanceName);
            if (consumeFromWhere != null) {
                consumer.setConsumeFromWhere(consumeFromWhere);
            }
            if (messageConsumeTimeout != null) {
                consumer.setConsumerPullTimeoutMillis(messageConsumeTimeout);
            }
            consumer.subscribe(topic, "*");
            consumer.setLanguage(LanguageCode.JAVA);
            MessageQueueListener origin = consumer.getMessageQueueListener();
            MessageListenerDelegator delegator = new MessageListenerDelegator(origin);
            consumer.setMessageQueueListener(delegator);
            return consumer;
        } catch (Exception e) {
            throw new RuntimeException("create pull consumer: " + instanceName + " failed", e);
        }
    }

    public static DefaultMQPushConsumer initDefaultMQPushConsumer(String namesrvAddr, String instanceName, String consumerGroup, String topic) {
        return initDefaultMQPushConsumer(false, null, null, namesrvAddr, instanceName, consumerGroup, null, null, null, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET, topic);
    }

    public static DefaultMQPushConsumer initDefaultMQPushConsumer(boolean aclEnable, String accessKey, String secretKey, String namesrvAddr, String instanceName, String consumerGroup, Integer maxReconsumeTimes, Long messageConsumeTimeout,
        Integer consumeThreadMin, ConsumeFromWhere where, String topic) {
        RPCHook rpcHook = null;
        if (aclEnable) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(rpcHook);
        try {
            consumer.setNamesrvAddr(namesrvAddr);
            consumer.setInstanceName(instanceName);
            consumer.setConsumerGroup(consumerGroup);
            if (maxReconsumeTimes != null) {
                consumer.setMaxReconsumeTimes(maxReconsumeTimes);
            }
            if (messageConsumeTimeout != null) {
                consumer.setConsumeTimeout(messageConsumeTimeout);
            }
            if (consumeThreadMin != null) {
                consumer.setConsumeThreadMin(consumeThreadMin);
            }
            if (where != null) {
                consumer.setConsumeFromWhere(where);
                if (where.equals(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP)) {
                    consumer.setConsumeTimestamp(UtilAll.timeMillisToHumanString3(System.currentTimeMillis()));
                }
            }
            consumer.subscribe(topic, "*");
            consumer.setLanguage(LanguageCode.JAVA);
            consumer.registerMessageListener((MessageListenerOrderly) (list, context) -> null);
        } catch (MQClientException e) {
            throw new RuntimeException("create push consumer: " + instanceName + " failed", e);
        }

        return consumer;
    }

    public static String createSubGroup(boolean aclEnable, String accessKey, String secretKey, String namesrvAddr, String adminExtGroup, String instanceName, String subGroup, String clusterName) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(aclEnable, accessKey, secretKey, namesrvAddr, adminExtGroup, instanceName);
            SubscriptionGroupConfig initConfig = new SubscriptionGroupConfig();
            initConfig.setGroupName(subGroup);

            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
            for (String addr : masterSet) {
                defaultMQAdminExt.createAndUpdateSubscriptionGroupConfig(addr, initConfig);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("create subGroup: " + subGroup + " failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return subGroup;
    }

    public static String createTopic(String namesrvAddr, String topic, Integer splitNum, String cluster, Boolean isOrder) {
        return createTopic(false, null, null, namesrvAddr, null, null, topic, splitNum, cluster, isOrder);
    }

    public static String createTopic(Boolean aclEnable, String accessKey, String secretKey, String namesrvAddr, String adminExtGroup, String instanceName, String topic, Integer splitNum, String clusterName, Boolean isOrder) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(aclEnable, accessKey, secretKey, namesrvAddr, adminExtGroup, instanceName);
            TopicConfig topicConfig = new TopicConfig();
            topicConfig.setReadQueueNums(splitNum);
            topicConfig.setWriteQueueNums(splitNum);
            topicConfig.setTopicName(topic.trim());
            topicConfig.setOrder(isOrder);

            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
            for (String addr : masterSet) {
                defaultMQAdminExt.createAndUpdateTopicConfig(addr, topicConfig);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("create subGroup: " + topic + " failed", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
        return topic;
    }

    public static void putKeyConfig(String namespace, String key, String value, String namesrvAddr) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(false, null, null, namesrvAddr, null, null);
            defaultMQAdminExt.createAndUpdateKvConfig(namespace, key, value);
        } catch (Exception e) {
            throw new RuntimeException("put key config error", e);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static String getKeyConfig(String namespace, String key, String namesrvAddr) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(false, null, null, namesrvAddr, null, null);
            return defaultMQAdminExt.getKVConfig(namespace, key);
        } catch (Exception e) {
            return null;
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static void deleteKeyConfig(String namespace, String key, String namesrvAddr) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(false, null, null, namesrvAddr, null, null);
            defaultMQAdminExt.deleteKvConfig(namespace, key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static KVTable getKVListByNameSpace(String namespace, String namesrvAddr) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(false, null, null, namesrvAddr, null, null);
            return defaultMQAdminExt.getKVListByNamespace(namespace);
        } catch (Exception e) {
            return null;
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static void resetOffsetNew(String namesrvAddr, String consumerGroup, String topic, long timestamp) {
        DefaultMQAdminExt defaultMQAdminExt = null;
        try {
            defaultMQAdminExt = startMQAdminTool(false, null, null, namesrvAddr, null, null);
            defaultMQAdminExt.resetOffsetByTimestamp(topic, consumerGroup, timestamp, true);
        } catch (Exception e) {
            System.err.printf("[%s] not exists, reset error%n", consumerGroup);
        } finally {
            if (defaultMQAdminExt != null) {
                defaultMQAdminExt.shutdown();
            }
        }
    }

    public static DefaultMQAdminExt startMQAdminTool(boolean aclEnable, String accessKey, String secretKey, String namesrvAddr, String adminExtGroup, String instanceName) throws MQClientException {
        RPCHook rpcHook = null;
        if (aclEnable) {
            rpcHook = new AclClientRPCHook(new SessionCredentials(accessKey, secretKey));
        }
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);
        defaultMQAdminExt.setNamesrvAddr(namesrvAddr);
        if (adminExtGroup != null) {
            defaultMQAdminExt.setAdminExtGroup(adminExtGroup);
        }
        if (instanceName != null) {
            defaultMQAdminExt.setInstanceName(instanceName);
        }

        defaultMQAdminExt.setMqClientApiTimeout(30000);
        defaultMQAdminExt.start();
        return defaultMQAdminExt;
    }

}

class MessageListenerDelegator implements MessageQueueListener {
    private final MessageQueueListener delegator;
    private final Set<MessageQueue> lastDivided = new HashSet<>();
    private final Set<MessageQueue> removingQueue = new HashSet<>();
    private final AtomicBoolean needSync = new AtomicBoolean(false);
    private final Object mutex = new Object();

    public MessageListenerDelegator(MessageQueueListener delegator) {
        this.delegator = delegator;
    }

    @Override public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        //上一次分配有，但是这一次没有,需要对这些mq进行状态移除
        for (MessageQueue last : lastDivided) {
            if (!mqDivided.contains(last)) {
                removingQueue.add(last);
            }
        }

        this.lastDivided.clear();
        this.lastDivided.addAll(mqDivided);

        needSync.set(true);
        delegator.messageQueueChanged(topic, mqAll, mqDivided);

        synchronized (this.mutex) {
            this.mutex.notifyAll();
        }
    }

    public Set<MessageQueue> getLastDivided() {
        return Collections.unmodifiableSet(this.lastDivided);
    }

    public Set<MessageQueue> getRemovingQueue() {
        return Collections.unmodifiableSet(this.removingQueue);
    }

    public boolean needSync() {
        return this.needSync.get();
    }

    public void hasSynchronized() {
        this.needSync.set(false);
    }

    public Object getMutex() {
        return mutex;
    }
}