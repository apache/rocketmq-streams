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

package org.apache.rocketmq.streams.source;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.PullRequest;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportShuffleSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.debug.DebugWriter;
import org.apache.rocketmq.streams.queue.RocketMQMessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class RocketMQSource extends AbstractSupportShuffleSource {

    protected static final Log LOG = LogFactory.getLog(RocketMQSource.class);

    private static final String STRATEGY_AVERAGE = "average";

    @ENVDependence
    protected String tags = SubscriptionData.SUB_ALL;

    protected Long pullIntervalMs;
    protected String strategyName;
    protected transient ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;//默认从哪里消费,不会被持久化。不设置默认从尾部消费

    protected transient PullConsumer pullConsumer;

    public RocketMQSource() {
    }

    public RocketMQSource(String topic, String tags, String groupName, String namesrvAddr) {
        this(topic, tags, groupName, namesrvAddr, STRATEGY_AVERAGE);
    }

    public RocketMQSource(String topic, String tags, String groupName, String namesrvAddr, String strategyName) {
        this.topic = topic;
        this.tags = tags;
        this.groupName = groupName;
        this.namesrvAddr = namesrvAddr;
        this.strategyName = strategyName;
    }

    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        try {
            destroyConsumer();
            this.pullConsumer = new PullConsumer(topic, groupName, namesrvAddr, tags, pullIntervalMs, consumeFromWhere);
            this.pullConsumer.start();

            return true;
        } catch (MQClientException e) {
            setInitSuccess(false);
            throw new RuntimeException("start rocketmq channel error " + topic, e);
        }
    }


    @Override
    public List<ISplit> getAllSplits() {
        try {
            List<ISplit> messageQueues = new ArrayList<>();
            Collection<MessageQueue> metaqQueueSet = this.pullConsumer.fetchMessageQueues(this.topic);
            for (MessageQueue queue : metaqQueueSet) {
                RocketMQMessageQueue metaqMessageQueue = new RocketMQMessageQueue(queue);
                messageQueues.add(metaqMessageQueue);
            }
            return messageQueues;
        } catch (MQClientException e) {
            e.printStackTrace();
            throw new RuntimeException("get all splits error ", e);
        }
    }

    //todo 计算正在工作的分片？
    @Override
    public Map<String, List<ISplit>> getWorkingSplitsGroupByInstances() {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setAdminExtGroup(UUID.randomUUID().toString());
        defaultMQAdminExt.setInstanceName(this.pullConsumer.getInstanceName());
        try {
            defaultMQAdminExt.start();
            Map<MessageQueue, String> queue2Instances = getMessageQueueAllocationResult(defaultMQAdminExt, this.groupName);
            Map<String, List<ISplit>> instanceOwnerQueues = new HashMap<>();
            for (MessageQueue messageQueue : queue2Instances.keySet()) {
                RocketMQMessageQueue metaqMessageQueue = new RocketMQMessageQueue(new MessageQueue(messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId()));
                if (isNotDataSplit(metaqMessageQueue.getQueueId())) {
                    continue;
                }
                String instanceName = queue2Instances.get(messageQueue);
                List<ISplit> splits = instanceOwnerQueues.computeIfAbsent(instanceName, k -> new ArrayList<>());
                splits.add(metaqMessageQueue);
            }
            return instanceOwnerQueues;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    private Map<MessageQueue, String> getMessageQueueAllocationResult(DefaultMQAdminExt defaultMQAdminExt,
                                                                      String groupName) {
        HashMap<MessageQueue, String> results = new HashMap<>();

        try {
            ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo(groupName);
            Iterator var5 = consumerConnection.getConnectionSet().iterator();

            while (var5.hasNext()) {
                Connection connection = (Connection) var5.next();
                String clientId = connection.getClientId();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(groupName, clientId, false);
                Iterator var9 = consumerRunningInfo.getMqTable().keySet().iterator();

                while (var9.hasNext()) {
                    MessageQueue messageQueue = (MessageQueue) var9.next();
                    results.put(messageQueue, clientId.split("@")[1]);
                }
            }
        } catch (Exception var11) {
            ;
        }

        return results;
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return queueId.toUpperCase().startsWith("RETRY") || queueId.toUpperCase().startsWith("%RETRY%");
    }

    @Override
    public boolean supportNewSplitFind() {
        return true;
    }

    @Override
    public boolean supportRemoveSplitFind() {
        return true;
    }

    @Override
    public boolean supportOffsetRest() {
        return false;
    }

    public void destroyConsumer() {
        this.pullConsumer.shutdown(true);
    }

    @Override
    public void destroy() {
        super.destroy();
        destroyConsumer();
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }


    public class PullConsumer extends ServiceThread {
        private final long pullIntervalMs;
        private final DefaultLitePullConsumer pullConsumer;

        public PullConsumer(String topic, String groupName, String namesrv,
                            String tags, long pullIntervalMs, ConsumeFromWhere consumeFromWhere) throws MQClientException {

            this.pullIntervalMs = pullIntervalMs;

            pullConsumer = new DefaultLitePullConsumer(groupName);
            pullConsumer.setNamesrvAddr(namesrv);
            pullConsumer.setConsumeFromWhere(consumeFromWhere);
            pullConsumer.subscribe(topic, tags);
            pullConsumer.setAutoCommit(false);

            MessageQueueListener origin = pullConsumer.getMessageQueueListener();
            MessageListenerDelegator delegator = new MessageListenerDelegator(origin, removingMessageQueue -> {
                Set<String> splitIds = new HashSet<>();
                for (MessageQueue mq : removingMessageQueue) {
                    splitIds.add(new RocketMQMessageQueue(mq).getQueueId());
                }

                RocketMQSource.this.removeSplit(splitIds);
            }, new Consumer<Set<MessageQueue>>() {
                @Override
                public void accept(Set<MessageQueue> messageQueues) {
                    //todo load 状态
                }
            });
            pullConsumer.setMessageQueueListener(delegator);

            pullConsumer.start();
        }

        @Override
        public void run() {

            while (!this.isStopped()) {
                try {
                    List<MessageExt> msgs = pullConsumer.poll(pullIntervalMs);

                    int i = 0;
                    for (MessageExt msg : msgs) {
                        JSONObject jsonObject = create(msg.getBody(), msg.getProperties());

                        String topic = msg.getTopic();
                        int queueId = msg.getQueueId();
                        String brokerName = msg.getBrokerName();
                        MessageQueue queue = new MessageQueue(topic, brokerName, queueId);
                        String unionQueueId = RocketMQMessageQueue.getQueueId(queue);


                        String offset = msg.getQueueOffset() + "";
                        org.apache.rocketmq.streams.common.context.Message message = createMessage(jsonObject, unionQueueId, offset, false);
                        message.getHeader().setOffsetIsLong(true);

                        if (i == msgs.size() - 1) {
                            message.getHeader().setNeedFlush(true);
                        }
                        executeMessage(message);
                        i++;
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }

        public Collection<MessageQueue> fetchMessageQueues(String topic) throws MQClientException {
            return this.pullConsumer.fetchMessageQueues(topic);
        }

        public String getInstanceName() {
            return this.pullConsumer.getInstanceName();
        }

        public void commit(Set<MessageQueue> messageQueues) {
            this.pullConsumer.commit(messageQueues, true);
        }

        @Override
        public void shutdown(boolean interrupt) {
            this.pullConsumer.shutdown();
            super.shutdown(interrupt);
        }

        @Override
        public String getServiceName() {
            return "RStreams-pull-thread";
        }
    }
}