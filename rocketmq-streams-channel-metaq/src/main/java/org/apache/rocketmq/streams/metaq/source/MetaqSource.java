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
package org.apache.rocketmq.streams.metaq.source;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.consumer.store.RemoteBrokerOffsetStore;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.impl.MQClientManager;
import com.alibaba.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.Connection;
import com.taobao.metaq.client.MetaPushConsumer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.streams.common.channel.source.AbstractPushSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.metaq.MetaqOffset;
import org.apache.rocketmq.streams.metaq.debug.DebugWriter;
import org.apache.rocketmq.streams.metaq.queue.MetaqMessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

public class MetaqSource extends AbstractPushSource {

    protected static final Log LOG = LogFactory.getLog(MetaqSource.class);
    @ENVDependence
    protected String tags = "*";                                       // 每个消息队列都会有一个线程
    protected Long pullIntervalMs;
    protected transient ConsumeFromWhere consumeFromWhere;//默认从哪里消费,不会被持久化。不设置默认从尾部消费
    protected transient String consumerOffset;//从哪里开始消费
    protected transient List<ISplit<?, ?>> messageQueues;//topic对应的queue全集
    private transient MetaPushConsumer consumer = null;

    public MetaqSource() {
    }

    public MetaqSource(String topic, String tags, String groupName) {
        setTopic(topic);
        setTags(tags);
        setGroupName(groupName);
    }

    public MetaqSource(String topic, String groupName) {
        setTopic(topic);
        setGroupName(groupName);
    }

    public static void main(String[] args) throws InterruptedException {
        MetaqSource metaqSource = new MetaqSource("TOPIC_DIPPER_SYSTEM_MSG_6", "fdsdf");
        metaqSource.init();
        metaqSource.start(new IStreamOperator() {
            @Override public Object doMessage(IMessage message, AbstractContext context) {
                // System.out.println(message.getMessageBody());
                return null;
            }
        });
        System.out.println(metaqSource.fetchAllSplits().size());
        while (true) {
            Map<String, List<ISplit<?, ?>>> map = metaqSource.getWorkingSplitsGroupByInstances();
            List<ISplit<?, ?>> ownerSplits = map.get(RuntimeUtil.getDipperInstanceId());
            int count = 0;
            if (ownerSplits != null) {
                count = ownerSplits.size();
            }
            int sum = 0;
            for (List<ISplit<?, ?>> splits : map.values()) {
                sum += splits.size();
            }
            System.out.println(count + "  " + sum);
            Thread.sleep(1000);
        }
    }

    @Override
    protected boolean startSource() {
        try {
            destroyConsumer();
            consumer = startConsumer();
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("start metaq channel error " + topic, e);
        }
    }

    protected MetaPushConsumer startConsumer() {
        try {
            MetaPushConsumer consumer = new MetaPushConsumer(groupName);
            consumer.getMetaPushConsumerImpl().setVipChannelEnabled(false);//add by 明亮
            if (pullIntervalMs != null) {
                consumer.setPullInterval(pullIntervalMs);
            }
            consumer.setInstanceName(RuntimeUtil.getDipperInstanceId());
            consumer.setConsumeThreadMax(maxThread);
            consumer.setConsumeThreadMin(maxThread);
            consumer.subscribe(topic, tags);
            consumer.setPersistConsumerOffsetInterval((int) this.checkpointTime);
            consumer.setConsumeMessageBatchMaxSize(maxFetchLogGroupSize);
            if (consumeFromWhere != null) {
                consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
                if (consumerOffset != null) {
                    consumer.setConsumeTimestamp(consumerOffset);
                }
            }
            Map<String, Boolean> isFirstDataForQueue = new HashMap<>();
            //            consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_TIMESTAMP);
            //consumer.setConsumeTimestamp("20210307094606");
            consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
                try {
                    Thread.currentThread().setName(Thread.currentThread().getName());
                    for (MessageExt msg : msgs) {
                        JSONObject jsonObject = create(msg.getBody(), msg.getProperties());
                        String queueId = MetaqMessageQueue.getQueueId(context.getMessageQueue());
                        String offset = msg.getQueueOffset() + "";
                        org.apache.rocketmq.streams.common.context.Message message = createMessage(jsonObject, queueId, offset, false);
                        message.getHeader().setOffsetIsLong(true);
                        if (DebugWriter.isOpenDebug()) {
                            Boolean isFirstData = isFirstDataForQueue.get(queueId);
                            if (isFirstData == null) {
                                synchronized (this) {
                                    isFirstData = isFirstDataForQueue.get(queueId);
                                    if (isFirstData == null) {
                                        isFirstDataForQueue.put(queueId, true);
                                    }
                                    DebugWriter.getInstance(getTopic()).receiveFirstData(queueId, msg.getQueueOffset());
                                }
                            }
                        }
                        executeMessage(message);
                    }
                } catch (Exception e) {
                    LOG.error("消费metaq报错：" + e, e);
                }

                return ConsumeOrderlyStatus.SUCCESS;// 返回消费成功
            });
            //设置offset的存储器，对原有的实现做包装，在提交offset前，发送系统消息
            setOffsetStore(consumer);
            consumer.start();

            //consumer.getOffsetStore();
            return consumer;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("start metaq channel error " + topic, e);
        }
    }

    @Override public Map<String, List<ISplit<?, ?>>> getWorkingSplitsGroupByInstances() {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setAdminExtGroup(UUID.randomUUID().toString());
        defaultMQAdminExt.setInstanceName(this.consumer.getInstanceName());
        try {
            defaultMQAdminExt.start();
            Map<org.apache.rocketmq.common.message.MessageQueue, String> queue2Instances = getMessageQueueAllocationResult(defaultMQAdminExt, this.groupName);
            Map<String, List<ISplit<?, ?>>> instanceOwnerQueues = new HashMap<>();
            for (org.apache.rocketmq.common.message.MessageQueue messageQueue : queue2Instances.keySet()) {
                MetaqMessageQueue metaqMessageQueue = new MetaqMessageQueue(new MessageQueue(messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId()));
                if (isNotDataSplit(metaqMessageQueue.getQueueId())) {
                    continue;
                }
                String instanceName = queue2Instances.get(messageQueue);
                List<ISplit<?, ?>> splits = instanceOwnerQueues.get(instanceName);
                if (splits == null) {
                    splits = new ArrayList<>();
                    instanceOwnerQueues.put(instanceName, splits);
                }
                splits.add(metaqMessageQueue);
            }
            return instanceOwnerQueues;

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    protected boolean isNotDataSplit(String queueId) {
        return queueId.toUpperCase().startsWith("%RETRY%");
    }

    /**
     * 设置offset存储，包装原有的RemoteBrokerOffsetStore，在保存offset前发送系统消息
     *
     * @param consumer
     */
    protected void setOffsetStore(MetaPushConsumer consumer) {
        DefaultMQPushConsumerImpl defaultMQPushConsumer = consumer.getMetaPushConsumerImpl().getDefaultMQPushConsumerImpl();
        MQClientInstance mQClientFactory = MQClientManager.getInstance().getAndCreateMQClientInstance(defaultMQPushConsumer.getDefaultMQPushConsumer(), null);
        RemoteBrokerOffsetStore offsetStore = new RemoteBrokerOffsetStore(mQClientFactory, groupName);
        consumer.setOffsetStore(new MetaqOffset(offsetStore, this));//每个一分钟运行一次
    }

    @Override protected boolean hasListenerSplitChanged() {
        return true;
    }

    @Override
    public List<ISplit<?, ?>> fetchAllSplits() {
        try {
            if (messageQueues == null || messageQueues.size() == 0) {
                Set<MessageQueue> metaqQueueSet = consumer.fetchSubscribeMessageQueues(this.topic);
                List<ISplit<?, ?>> queueList = new ArrayList<>();
                for (MessageQueue queue : metaqQueueSet) {
                    MetaqMessageQueue metaqMessageQueue = new MetaqMessageQueue(queue);
                    if (isNotDataSplit(metaqMessageQueue.getQueueId())) {
                        continue;
                    }

                    queueList.add(metaqMessageQueue);

                }
                messageQueues = queueList;
            }
            return messageQueues;
        } catch (MQClientException e) {
            e.printStackTrace();
            throw new RuntimeException("get all splits error ", e);
        }
    }

    protected Map<org.apache.rocketmq.common.message.MessageQueue, String> getMessageQueueAllocationResult(DefaultMQAdminExt defaultMQAdminExt, String groupName) {
        HashMap results = new HashMap();

        try {
            ConsumerConnection consumerConnection = defaultMQAdminExt.examineConsumerConnectionInfo(groupName);
            Iterator var5 = consumerConnection.getConnectionSet().iterator();

            while (var5.hasNext()) {
                Connection connection = (Connection) var5.next();
                String clientId = connection.getClientId();
                ConsumerRunningInfo consumerRunningInfo = defaultMQAdminExt.getConsumerRunningInfo(groupName, clientId, false);
                Iterator var9 = consumerRunningInfo.getMqTable().keySet().iterator();

                while (var9.hasNext()) {
                    org.apache.rocketmq.common.message.MessageQueue messageQueue = (org.apache.rocketmq.common.message.MessageQueue) var9.next();
                    results.put(messageQueue, clientId.split("@")[1]);
                }
            }
        } catch (Exception var11) {
            ;
        }

        return results;
    }

    public void destroyConsumer() {
        List<MetaPushConsumer> oldConsumers = new ArrayList<>();
        if (consumer != null) {
            oldConsumers.add(consumer);
        }

        try {
            for (MetaPushConsumer consumer : oldConsumers) {
                consumer.shutdown();
            }

        } catch (Throwable t) {
            if (LOG.isWarnEnabled()) {
                LOG.warn(t.getMessage(), t);
            }
        }

    }

    @Override
    public void destroySource() {
        destroyConsumer();
    }

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public void setConsumerWhere(String offsetTime) {
        if (StringUtil.isNotEmpty(offsetTime)) {
            this.consumeFromWhere = ConsumeFromWhere.CONSUME_FROM_TIMESTAMP;
            this.consumerOffset = offsetTime;
        }
    }

    public Long getPullIntervalMs() {
        return pullIntervalMs;
    }

    public void setPullIntervalMs(Long pullIntervalMs) {
        this.pullIntervalMs = pullIntervalMs;
    }
}
