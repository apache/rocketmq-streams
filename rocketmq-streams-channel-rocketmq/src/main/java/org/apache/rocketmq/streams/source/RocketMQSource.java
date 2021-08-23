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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.AccessChannel;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.ProcessQueue;
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
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportOffsetResetSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.RocketMQOffset;
import org.apache.rocketmq.streams.queue.RocketMQMessageQueue;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class RocketMQSource extends AbstractSupportOffsetResetSource {

    protected static final Log LOG = LogFactory.getLog(RocketMQSource.class);
    @ENVDependence
    protected String tags = "*";

    private transient DefaultMQPushConsumer consumer;

    protected Long pullIntervalMs;

    /**
     * 消息队列命名空间接入点
     */
    protected String namesrvAddr;

    protected transient ConsumeFromWhere consumeFromWhere;//默认从哪里消费,不会被持久化。不设置默认从尾部消费
    protected transient String consumerOffset;//从哪里开始消费

    public RocketMQSource() {}

    public RocketMQSource(String topic, String tags, String groupName, String endpoint,
                          String namesrvAddr, String accessKey, String secretKey, String instanceId) {
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
            consumer=startConsumer();
            return true;
        } catch (Exception e) {
            setInitSuccess(false);
            e.printStackTrace();
            throw new RuntimeException("start rocketmq channel error " + topic, e);
        }
    }

    protected DefaultMQPushConsumer startConsumer() {
        try {
            DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(groupName);
            if (pullIntervalMs != null) {
                consumer.setPullInterval(pullIntervalMs);
            }
            //            consumer.setConsumeThreadMax(maxThread);
            //            consumer.setConsumeThreadMin(maxThread);

            consumer.setPersistConsumerOffsetInterval((int)this.checkpointTime);
            consumer.setConsumeMessageBatchMaxSize(maxFetchLogGroupSize);
            consumer.setAccessChannel(AccessChannel.CLOUD);
            consumer.setNamesrvAddr(this.namesrvAddr);
            if (consumeFromWhere != null) {
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
                if (consumerOffset != null) {
                    consumer.setConsumeTimestamp(consumerOffset);
                }
            }

            consumer.subscribe(topic, tags);
            consumer.registerMessageListener((MessageListenerOrderly)(msgs, context) -> {
                try {
                    int i = 0;
                    for (MessageExt msg : msgs) {
                        String data = new String(msg.getBody(), CHARSET);
                        JSONObject jsonObject = create(data);
                        String queueId = RocketMQMessageQueue.getQueueId(context.getMessageQueue());
                        String offset = msg.getQueueOffset() + "";
                        org.apache.rocketmq.streams.common.context.Message message = createMessage(jsonObject, queueId, offset, false);
                        message.getHeader().setOffsetIsLong(true);
                        //                        message.getHeader().setQueueId(RocketMQMessageQueue.getQueueId(context.getMessageQueue()));
                        //                        message.getHeader().setOffset(String.valueOf(msg.getQueueOffset()));
                        //                        message.getHeader().setMessageQueue(new RocketMQMessageQueue(context.getMessageQueue()));
                        if (i == msgs.size() - 1) {
                            message.getHeader().setNeedFlush(true);
                        }
                        executeMessage(message);
                        i++;
                    }
                } catch (Exception e) {
                    LOG.error("消费rocketmq报错：" + e, e);
                }

                return ConsumeOrderlyStatus.SUCCESS;// 返回消费成功
            });

            setOffsetStore(consumer);
            consumer.start();

            return consumer;
        } catch (Exception e) {
            setInitSuccess(false);
            e.printStackTrace();
            throw new RuntimeException("start rocketmq channel error " + topic, e);
        }
    }
    @Override
    public List<ISplit> getAllSplits(){
        try {
            Set<MessageQueue> rocketmqQueueSet =  consumer.fetchSubscribeMessageQueues(this.topic);
            List<ISplit> queueList = new ArrayList<>();
            for (MessageQueue queue : rocketmqQueueSet) {
                RocketMQMessageQueue rocketmqMessageQueue = new RocketMQMessageQueue(queue);
                if(isNotDataSplit(rocketmqMessageQueue.getQueueId())){
                    continue;
                }

                queueList.add(rocketmqMessageQueue);

            }
            return queueList;
        } catch (MQClientException e) {
            e.printStackTrace();
            throw new RuntimeException("get all splits error ",e);
        }
    }


    @Override
    public Map<String,List<ISplit>> getWorkingSplitsGroupByInstances(){
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setAdminExtGroup(UUID.randomUUID().toString());
        defaultMQAdminExt.setInstanceName(this.consumer.getInstanceName());
        try {
            defaultMQAdminExt.start();
            Map<org.apache.rocketmq.common.message.MessageQueue, String> queue2Instances= getMessageQueueAllocationResult(defaultMQAdminExt,this.groupName);
            Map<String,List<ISplit>> instanceOwnerQueues=new HashMap<>();
            for(org.apache.rocketmq.common.message.MessageQueue messageQueue:queue2Instances.keySet()){
                RocketMQMessageQueue rocketmqMessageQueue = new RocketMQMessageQueue(new MessageQueue(messageQueue.getTopic(),messageQueue.getBrokerName(),messageQueue.getQueueId()));
                if(isNotDataSplit(rocketmqMessageQueue.getQueueId())){
                    continue;
                }
                String instanceName=queue2Instances.get(messageQueue);
                List<ISplit> splits=instanceOwnerQueues.get(instanceName);
                if(splits==null){
                    splits=new ArrayList<>();
                    instanceOwnerQueues.put(instanceName,splits);
                }
                splits.add(rocketmqMessageQueue);
            }
            return instanceOwnerQueues;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
    protected Map<org.apache.rocketmq.common.message.MessageQueue, String> getMessageQueueAllocationResult(DefaultMQAdminExt defaultMQAdminExt, String groupName) {
        HashMap results = new HashMap();

        try {
            ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo(groupName);
            Iterator var5 = consumerConnection.getConnectionSet().iterator();

            while(var5.hasNext()) {
                Connection connection = (Connection)var5.next();
                String clientId = connection.getClientId();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(groupName, clientId, false);
                Iterator var9 = consumerRunningInfo.getMqTable().keySet().iterator();

                while(var9.hasNext()) {
                    org.apache.rocketmq.common.message.MessageQueue messageQueue = (org.apache.rocketmq.common.message.MessageQueue)var9.next();
                    results.put(messageQueue, clientId.split("@")[1]);
                }
            }
        } catch (Exception var11) {
            ;
        }

        return results;
    }

    /**
     * 设置offset存储，包装原有的RemoteBrokerOffsetStore，在保存offset前发送系统消息
     *
     * @param consumer
     */
    protected void setOffsetStore(DefaultMQPushConsumer consumer) {
        DefaultMQPushConsumerImpl defaultMQPushConsumer = consumer.getDefaultMQPushConsumerImpl();
        if (consumer.getMessageModel() == MessageModel.CLUSTERING) {
            consumer.changeInstanceNameToPID();
        }
        MQClientInstance mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(defaultMQPushConsumer.getDefaultMQPushConsumer());
        RemoteBrokerOffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, NamespaceUtil.wrapNamespace(consumer.getNamespace(), consumer.getConsumerGroup()));
        consumer.setOffsetStore(new RocketMQOffset(offsetStore, this));//每个一分钟运行一次
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return queueId.toUpperCase().startsWith("%RETRY%");
    }

    /**
     * 分片发生变化时，回调系统函数，发送系统消息，告知各个组件
     *
     * @param consumer
     */
    protected void addRebalanceCallback(DefaultMQPushConsumer consumer) {
        DefaultMQPushConsumerImpl defaultMQPushConsumerImpl = consumer.getDefaultMQPushConsumerImpl();
        // DefaultMQPushConsumerImpl defaultMQPushConsumerImpl=metaPushConsumer.getDefaultMQPushConsumerImpl();
        ReflectUtil.setBeanFieldValue(defaultMQPushConsumerImpl, "rebalanceImpl", new RebalancePushImpl(defaultMQPushConsumerImpl) {
            @Override
            public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
                Set<String> queueIds = new HashSet<>();
                for (MessageQueue messageQueue : mqAll) {
                    if (!mqDivided.contains(messageQueue)) {
                        ProcessQueue pq = this.processQueueTable.remove(messageQueue);
                        if (pq != null) {
                            pq.setDropped(true);
                            log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, messageQueue);
                        }
                        queueIds.add(RocketMQMessageQueue.getQueueId(messageQueue));
                    }
                }
                Set<String> newQueueIds = new HashSet<>();
                for (MessageQueue messageQueue : mqDivided) {
                    if (!mqAll.contains(messageQueue)) {
                        newQueueIds.add(RocketMQMessageQueue.getQueueId(messageQueue));
                    }
                }
                removeSplit(queueIds);
                addNewSplit(newQueueIds);

            }
        });
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
        List<DefaultMQPushConsumer> oldConsumers = new ArrayList<>();
        if(consumer!=null){
            oldConsumers.add(consumer);
        }
        try {
            for (DefaultMQPushConsumer consumer : oldConsumers) {
                consumer.shutdown();
            }

        } catch (Throwable t) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(t.getMessage(), t);
            }
        }

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
}