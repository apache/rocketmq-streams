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

package org.apache.rocketmq.streams.rocketmq.source;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.AllocateMessageQueueStrategy;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueAveragely;
import org.apache.rocketmq.client.consumer.rebalance.AllocateMessageQueueByConfig;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.common.channel.source.AbstractPushSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.rocketmq.debug.DebugWriter;
import org.apache.rocketmq.streams.rocketmq.queue.RocketMQMessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketMQSource extends AbstractPushSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(RocketMQSource.class);

    private static final String STRATEGY_AVERAGE = "average";
    private static final String STRATEGY_MACHINE = "machine";
    private static final String STRATEGY_CONFIG = "config";

    @ENVDependence protected String tags = "*";

    /**
     * 消息队列命名空间接入点
     */
    protected String namesrvAddr;

    protected Long pullIntervalMs;

    protected String strategyName;

    protected String instanceName;

    protected boolean isMessageListenerConcurrently = false;//当有窗口时，不建议用concurrent，会有丢数据的情况

    protected transient DefaultMQPushConsumer consumer;
    protected transient ConsumeFromWhere consumeFromWhere;//默认从哪里消费,不会被持久化。不设置默认从尾部消费
    protected transient String consumerOffset;//从哪里开始消费

    public RocketMQSource() {
        this.instanceName = RuntimeUtil.getDipperInstanceId();
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
        this.instanceName = RuntimeUtil.getDipperInstanceId();
    }

    @Override protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override protected boolean startSource() {
        try {
            if (this.consumer == null) {
                this.consumer = new DefaultMQPushConsumer(groupName);
                DefaultMQPushConsumerImpl impl = ReflectUtil.getDeclaredField(this.consumer, "defaultMQPushConsumerImpl");

                ReflectUtil.setBeanFieldValue(impl, "rebalanceImpl", new RebalancePushImpl(impl) {

                    @Override public boolean doRebalance(boolean isOrder) {
                        super.doRebalance(isOrder);
                        List<String> cid = super.mQClientFactory.findConsumerIdList(topic, groupName);
                        doDispatch(topic, cid);
                        return true;
                    }
                });

                this.consumer.setInstanceName(this.instanceName);
                if (pullIntervalMs != null) {
                    this.consumer.setPullInterval(pullIntervalMs);
                }
                AllocateMessageQueueStrategy defaultStrategy = new AllocateMessageQueueAveragely();
                if (STRATEGY_AVERAGE.equalsIgnoreCase(this.strategyName)) {
                    this.consumer.setAllocateMessageQueueStrategy(defaultStrategy);
                } else if (STRATEGY_MACHINE.equalsIgnoreCase(this.strategyName)) {
                    //consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueByMachine(defaultStrategy));
                } else if (STRATEGY_CONFIG.equalsIgnoreCase(this.strategyName)) {
                    this.consumer.setAllocateMessageQueueStrategy(new AllocateMessageQueueByConfig());
                }

                this.consumer.setPersistConsumerOffsetInterval((int) this.checkpointTime);
                this.consumer.setConsumeMessageBatchMaxSize(maxFetchLogGroupSize);
                this.consumer.setNamesrvAddr(this.namesrvAddr);
                if (consumeFromWhere != null) {
                    this.consumer.setConsumeFromWhere(consumeFromWhere);
                    if (consumerOffset != null) {
                        this.consumer.setConsumeTimestamp(consumerOffset);
                    }
                } else {
                    this.consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
                }
                //consumer.setCommitOffsetWithPullRequestEnable(false);
                this.consumer.subscribe(topic, tags);
                if (isMessageListenerConcurrently) {
                    this.consumer.registerMessageListener(new MessageListenerConcurrently() {
                        @Override public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                            ConsumeConcurrentlyContext context) {
                            processMessage(msgs, context.getMessageQueue());
                            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                        }
                    });
                } else {
                    this.consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
                        processMessage(msgs, context.getMessageQueue());
                        return ConsumeOrderlyStatus.SUCCESS;// 返回消费成功
                    });
                }

                setOffsetStore(this.consumer);
            }
            this.consumer.start();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("start rocketmq channel error " + topic, e);
        }
    }

    protected void processMessage(List<MessageExt> msgs, MessageQueue messageQueue) {
        try {
            int i = 0;
            for (MessageExt msg : msgs) {

                JSONObject jsonObject = create(msg.getBody(), msg.getProperties());

                String queueId = RocketMQMessageQueue.getQueueId(messageQueue);
                String offset = msg.getQueueOffset() + "";
                org.apache.rocketmq.streams.common.context.Message message = createMessage(jsonObject, queueId, offset, false);
                message.getHeader().setOffsetIsLong(true);

                if (i == msgs.size() - 1) {
                    message.getHeader().setNeedFlush(true);
                }
                executeMessage(message);
                i++;
            }
        } catch (Exception e) {
            LOGGER.error("consume message from rocketmq error " + e, e);
            e.printStackTrace();
        }

    }

    /**
     * 用于任务分发的接口， 默认source不会使用该方法
     *
     * @param topic          用于分发的topic
     * @param consumerIdList 消费者ID列表
     */
    protected void doDispatch(String topic, List<String> consumerIdList) {
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        try {
            List<ISplit<?, ?>> messageQueues = new ArrayList<>();
            Set<MessageQueue> metaqQueueSet = consumer.fetchSubscribeMessageQueues(this.topic);
            for (MessageQueue queue : metaqQueueSet) {
                RocketMQMessageQueue metaqMessageQueue = new RocketMQMessageQueue(queue);
                if (isNotDataSplit(metaqMessageQueue.getQueueId())) {
                    continue;
                }
                messageQueues.add(metaqMessageQueue);
            }
            return messageQueues;
        } catch (MQClientException e) {
            e.printStackTrace();
            throw new RuntimeException("get all splits error ", e);
        }
    }

    @Override public Map<String, List<ISplit<?, ?>>> getWorkingSplitsGroupByInstances() {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setAdminExtGroup(UUID.randomUUID().toString());
        defaultMQAdminExt.setInstanceName(this.consumer.getInstanceName());
        try {
            defaultMQAdminExt.start();
            Map<MessageQueue, String> queue2Instances = getMessageQueueAllocationResult(defaultMQAdminExt, this.groupName);
            Map<String, List<ISplit<?, ?>>> instanceOwnerQueues = new HashMap<>();
            for (MessageQueue messageQueue : queue2Instances.keySet()) {
                RocketMQMessageQueue metaqMessageQueue = new RocketMQMessageQueue(new MessageQueue(messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId()));
                if (isNotDataSplit(metaqMessageQueue.getQueueId())) {
                    continue;
                }
                String instanceName = queue2Instances.get(messageQueue);
                List<ISplit<?, ?>> splits = instanceOwnerQueues.computeIfAbsent(instanceName, k -> new ArrayList<>());
                splits.add(metaqMessageQueue);
            }
            return instanceOwnerQueues;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    protected Map<MessageQueue, String> getMessageQueueAllocationResult(DefaultMQAdminExt defaultMQAdminExt, String groupName) {
        Map<MessageQueue, String> results = new HashMap<>();

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
            var11.printStackTrace();
        }

        return results;
    }

    /**
     * 设置offset存储，包装原有的RemoteBrokerOffsetStore，在保存offset前发送系统消息 this method suggest to be removed, use check barrier to achieve checkpoint asynchronous.
     *
     * @param consumer
     */
    protected void setOffsetStore(DefaultMQPushConsumer consumer) {
        DefaultMQPushConsumerImpl defaultMQPushConsumer = consumer.getDefaultMQPushConsumerImpl();
        if (consumer.getMessageModel() == MessageModel.CLUSTERING) {
            consumer.changeInstanceNameToPID();
        }
        MQClientInstance mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(defaultMQPushConsumer.getDefaultMQPushConsumer());
        RemoteBrokerOffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, NamespaceUtil.wrapNamespace(consumer.getNamespace(), consumer.getConsumerGroup())) {

            @Override public void removeOffset(MessageQueue mq) {
                Set<String> splitIds = new HashSet<>();
                splitIds.add(new RocketMQMessageQueue(mq).getQueueId());
                removeSplit(splitIds);
                super.removeOffset(mq);
            }

            @Override public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
                sendCheckpoint(new RocketMQMessageQueue(mq).getQueueId());
                if (DebugWriter.isOpenDebug()) {
                    ConcurrentMap<MessageQueue, AtomicLong> offsetTable = ReflectUtil.getDeclaredField(this, "offsetTable");
                    DebugWriter.getInstance(getTopic()).writeSaveOffset(mq, offsetTable.get(mq));
                }
                //                LOG.info("the queue Id is " + new RocketMQMessageQueue(mq).getQueueId() + ",rocketmq start save offset，the save time is " + DateUtil.getCurrentTimeString());
                super.updateConsumeOffsetToBroker(mq, offset, isOneway);
            }
        };
        consumer.setOffsetStore(offsetStore);//每个一分钟运行一次
    }

    protected boolean isNotDataSplit(String queueId) {
        return queueId.toUpperCase().startsWith("RETRY") || queueId.toUpperCase().startsWith("%RETRY%");
    }

    @Override public void destroySource() {
        if (this.consumer != null) {
            this.consumer.shutdown();
            this.consumer = null;
        }
    }

    @Override protected boolean hasListenerSplitChanged() {
        return true;
    }

    public String getInstanceName() {
        return instanceName;
    }

    public String getStrategyName() {
        return strategyName;
    }

    public void setStrategyName(String strategyName) {
        this.strategyName = strategyName;
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public DefaultMQPushConsumer getConsumer() {
        return consumer;
    }

    public Long getPullIntervalMs() {
        return pullIntervalMs;
    }

    public void setPullIntervalMs(Long pullIntervalMs) {
        this.pullIntervalMs = pullIntervalMs;
    }

    public String getNamesrvAddr() {
        return namesrvAddr;
    }

    public void setNamesrvAddr(String namesrvAddr) {
        this.namesrvAddr = namesrvAddr;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }

    public String getConsumerOffset() {
        return consumerOffset;
    }

    public void setConsumerOffset(String consumerOffset) {
        this.consumerOffset = consumerOffset;
    }

    public boolean isMessageListenerConcurrently() {
        return isMessageListenerConcurrently;
    }

    public void setMessageListenerConcurrently(boolean messageListenerConcurrently) {
        isMessageListenerConcurrently = messageListenerConcurrently;
    }
}