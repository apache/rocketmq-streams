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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportShuffleSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.apache.rocketmq.streams.queue.RocketMQMessageQueue;
import org.apache.rocketmq.streams.schema.SchemaConfig;
import org.apache.rocketmq.streams.schema.SchemaType;
import org.apache.rocketmq.streams.schema.SchemaWrapper;
import org.apache.rocketmq.streams.schema.SchemaWrapperFactory;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class RocketMQSource extends AbstractSupportShuffleSource {

    protected static final Log LOG = LogFactory.getLog(RocketMQSource.class);

    @ENVDependence
    private String tags = SubscriptionData.SUB_ALL;

    private int userPullThreadNum = 1;
    private long pullTimeout;
    private long commitInternalMs = 1000;

    private SchemaConfig schemaConfig;
    /**
     * 默认从哪里消费,不会被持久化。不设置默认从尾部消费
     */
    private transient ConsumeFromWhere consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET;
    private RPCHook rpcHook;
    private transient DefaultLitePullConsumer pullConsumer;
    private transient ExecutorService executorService;
    private transient PullTask[] pullTasks;
    private transient volatile AtomicBoolean committing = new AtomicBoolean(false);

    public RocketMQSource() {
    }

    public RocketMQSource(String topic, String tags, String groupName, String namesrvAddr) {
        this.topic = topic;
        this.tags = tags;
        this.groupName = groupName;
        this.namesrvAddr = namesrvAddr;
    }

    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        try {
            destroyConsumer();

            this.pullConsumer = buildPullConsumer(topic, groupName, namesrvAddr, tags, rpcHook, consumeFromWhere);

            if (this.executorService == null) {
                this.executorService = new ThreadPoolExecutor(userPullThreadNum, userPullThreadNum, 0, TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(1000), r -> new Thread(r, "RStream-poll-thread"));
            }

            pullTasks = new PullTask[userPullThreadNum];
            for (int i = 0; i < userPullThreadNum; i++) {
                pullTasks[i] = new PullTask(this.pullConsumer, pullTimeout, commitInternalMs);
                this.executorService.execute(pullTasks[i]);
            }

            this.pullConsumer.start();

            return true;
        } catch (Throwable t) {
            setInitSuccess(false);
            throw new RuntimeException("start rocketmq channel error " + topic, t);
        }
    }

    private DefaultLitePullConsumer buildPullConsumer(String topic, String groupName, String namesrv, String tags,
                                                      RPCHook rpcHook, ConsumeFromWhere consumeFromWhere) throws MQClientException {
        DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(groupName, rpcHook);
        pullConsumer.setNamesrvAddr(namesrv);
        pullConsumer.setConsumeFromWhere(consumeFromWhere);
        pullConsumer.subscribe(topic, tags);
        pullConsumer.setAutoCommit(false);
        pullConsumer.setPullBatchSize(1000);

        MessageQueueListener origin = pullConsumer.getMessageQueueListener();

        MessageListenerDelegator delegator = new MessageListenerDelegator(origin);

        pullConsumer.setMessageQueueListener(delegator);

        return pullConsumer;
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
            Iterator iterator = consumerConnection.getConnectionSet().iterator();

            while (iterator.hasNext()) {
                Connection connection = (Connection) iterator.next();
                String clientId = connection.getClientId();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(groupName, clientId, false);
                Iterator iterator1 = consumerRunningInfo.getMqTable().keySet().iterator();

                while (iterator1.hasNext()) {
                    MessageQueue messageQueue = (MessageQueue) iterator1.next();
                    results.put(messageQueue, clientId.split("@")[1]);
                }
            }
        } catch (Exception ex) {
            ;
        }

        return results;
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return false;
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
        if (this.pullConsumer == null || this.pullTasks == null || this.pullTasks.length == 0) {
            return;
        }

        //不在拉取新的数据
        for (PullTask pullTask : pullTasks) {
            pullTask.shutdown();
        }

        //线程池关闭
        this.executorService.shutdown();

        //关闭消费实例
        try {
            synchronized (committing) {
                while (committing.get()) {
                    committing.wait();
                }
            }

            this.pullConsumer.shutdown();
        } catch (Throwable t) {
            LOG.error(t);
        }

    }

    public void commit(Set<MessageQueue> messageQueues) {
        if (this.pullConsumer.isRunning()) {
            synchronized (committing) {
                committing.set(true);
                this.pullConsumer.commit(messageQueues, true);
                committing.set(false);
                committing.notifyAll();
            }
        }
    }

    @Override
    public void destroy() {
        super.destroy();
        destroyConsumer();
    }

    public class PullTask implements Runnable {
        private final long pullTimeout;
        private final long commitInternalMs;
        private volatile long lastCommit = 0L;

        private final DefaultLitePullConsumer pullConsumer;
        private final MessageListenerDelegator delegator;

        private volatile boolean isStopped = false;

        public PullTask(DefaultLitePullConsumer pullConsumer, long pullTimeout, long commitInternalMs) {
            this.pullConsumer = pullConsumer;
            this.delegator = (MessageListenerDelegator) pullConsumer.getMessageQueueListener();
            this.pullTimeout = pullTimeout == 0 ? pullConsumer.getPollTimeoutMillis() : pullTimeout;
            this.commitInternalMs = commitInternalMs;
        }

        private void afterRebalance() {
            //if rebalance happen, need block all other thread, wait remove split or load states from new split;
            Set<MessageQueue> removingQueue = this.delegator.getRemovingQueue();

            Set<String> splitIds = new HashSet<>();
            for (MessageQueue mq : removingQueue) {
                splitIds.add(new RocketMQMessageQueue(mq).getQueueId());
            }

            RocketMQSource.this.removeSplit(splitIds);

            Set<MessageQueue> allQueueInLastRebalance = this.delegator.getLastDivided();
            newRebalance(allQueueInLastRebalance);

            this.delegator.hasSynchronized();
        }

        @Override
        public void run() {

            try {
                //wait rebalance
                synchronized (this.delegator.getMutex()) {
                    this.delegator.getMutex().wait();
                }
                afterRebalance();
            } catch (InterruptedException ignored) {
            }

            while (!this.isStopped) {
                try {
                    if (this.delegator.needSync()) {
                        synchronized (this.pullConsumer) {
                            if (this.delegator.needSync()) {
                                afterRebalance();
                            }
                        }
                    }


                    List<MessageExt> msgs = pullConsumer.poll(pullTimeout);

                int i = 0;
                for (MessageExt msg : msgs) {
                    JSONObject jsonObject = createFromMsg(msg);

                        String topic = msg.getTopic();
                        int queueId = msg.getQueueId();
                        String brokerName = msg.getBrokerName();
                        MessageQueue queue = new MessageQueue(topic, brokerName, queueId);
                        String unionQueueId = RocketMQMessageQueue.getQueueId(queue);


                    String offset = msg.getQueueOffset() + "";
                    org.apache.rocketmq.streams.common.context.Message message =
                        createMessage(jsonObject, unionQueueId, offset, false);
                    message.getHeader().setOffsetIsLong(true);

                        if (i == msgs.size() - 1) {
                            message.getHeader().setNeedFlush(true);
                        }
                        executeMessage(message);
                        i++;
                    }

                    //拉取的批量消息处理完成以后判断是否提交位点；
                    synchronized (this.pullConsumer) {
                        if (System.currentTimeMillis() - lastCommit >= commitInternalMs && !isStopped) {
                            lastCommit = System.currentTimeMillis();
                            //向broker提交消费位点,todo 从consumer那里拿不到正在消费哪些messageQueue
                            commit(this.delegator.getLastDivided());
                        }
                    }
                } catch (Throwable t) {
                    LOG.error(t);
                }
            }
        }

        public void shutdown() {
            Set<MessageQueue> lastDivided = this.delegator.getLastDivided();
            if (lastDivided != null && lastDivided.size() != 0) {
                commit(lastDivided);
            }

            this.isStopped = true;
        }
    }

    /**
     * 从 rocketmq 消息转为可被后续环节处理的jsonObject
     * @param messageExt
     * @return
     */
    public JSONObject createFromMsg(MessageExt messageExt) {
        if (schemaConfig != null && !isJsonData) {
            try {
                SchemaWrapper schemaWrapper =
                    SchemaWrapperFactory.createIfAbsent(messageExt.getTopic(), schemaConfig);
                Object pojo = schemaWrapper.deserialize(messageExt);
                if (SchemaType.STRING.name().equals(schemaConfig.getSchemaType())) {
                    return new UserDefinedMessage(pojo, Lists.newArrayList(IMessage.DATA_KEY));
                }
                return new UserDefinedMessage(pojo);
            } catch (Exception ex) {
                LOG.error("deserialize with schema failed, try to deserialize directly to json", ex);
            }
        }
        return create(messageExt.getBody(), messageExt.getProperties());
    }

    private void newRebalance(Set<MessageQueue> allQueueInLastRebalance){
        Set<String> temp = new HashSet<>();
        for (MessageQueue queue : allQueueInLastRebalance) {
            String unionQueueId = RocketMQMessageQueue.getQueueId(queue);
            temp.add(unionQueueId);
        }

        super.addNewSplit(temp);
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public Long getPullTimeout() {
        return pullTimeout;
    }

    public void setPullTimeout(Long pullTimeout) {
        this.pullTimeout = pullTimeout;
    }

    public RPCHook getRpcHook() {
        return rpcHook;
    }

    public void setRpcHook(RPCHook rpcHook) {
        this.rpcHook = rpcHook;
    }

    public int getUserPullThreadNum() {
        return userPullThreadNum;
    }

    public void setUserPullThreadNum(int userPullThreadNum) {
        this.userPullThreadNum = userPullThreadNum;
    }

    public long getCommitInternalMs() {
        return commitInternalMs;
    }

    public void setCommitInternalMs(long commitInternalMs) {
        this.commitInternalMs = commitInternalMs;
    }

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }
}