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

package org.apache.rocketmq.streams.sink;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.queue.RocketMQMessageQueue;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.*;

public class RocketMQSink extends AbstractSupportShuffleSink {

    private static final Log LOG = LogFactory.getLog(RocketMQSink.class);
    @ENVDependence
    private String tags = "*";

    private String topic;
    private String groupName;

    private transient List<DefaultMQPushConsumer> consumers = new ArrayList<>();
    private transient DefaultMQProducer producer;

    private Long pullIntervalMs;
    private String namesrvAddr;

    public RocketMQSink() {
    }

    public RocketMQSink(String namesrvAddr, String topic, String groupName) {
        this.namesrvAddr = namesrvAddr;
        this.topic = topic;
        this.groupName = groupName;
    }


    @Override
    protected boolean initConfigurable() {
        super.initConfigurable();
        return true;
    }

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        if (messages == null) {
            return true;
        }
        if (StringUtil.isEmpty(topic)) {
            if (LOG.isErrorEnabled()) {
                LOG.error("topic is blank");
            }
            return false;
        }
        initProducer();

        try {
            Map<String, List<Message>> msgsByQueueId = new HashMap<>();// group by queueId, if the message not contains queue info ,the set default string as default queueId
            Map<String, MessageQueue> messageQueueMap = new HashMap<>();//if has queue id in message, save the map for queueid 2 messagequeeue
            String defaultQueueId = "<null>";//message is not contains queue ,use default
            for (IMessage msg : messages) {
                ISplit<RocketMQMessageQueue, MessageQueue> channelQueue = getSplit(msg);
                String queueId = defaultQueueId;
                if (channelQueue != null) {
                    queueId = channelQueue.getQueueId();
                    RocketMQMessageQueue metaqMessageQueue = (RocketMQMessageQueue) channelQueue;
                    messageQueueMap.put(queueId, metaqMessageQueue.getQueue());
                }
                List<Message> messageList = msgsByQueueId.get(queueId);
                if (messageList == null) {
                    messageList = new ArrayList<>();
                    msgsByQueueId.put(queueId, messageList);
                }
                messageList.add(new Message(topic, tags, null, msg.getMessageBody().toJSONString().getBytes("UTF-8")));
            }
            List<Message> messageList = msgsByQueueId.get(defaultQueueId);
            if (messageList != null) {
                for (Message message : messageList) {
                    producer.sendOneway(message);
                }
                messageQueueMap.remove(defaultQueueId);
            }
            if (messageQueueMap.size() <= 0) {
                return true;
            }
            for (String queueId : msgsByQueueId.keySet()) {
                messageList = msgsByQueueId.get(queueId);
                for (Message message : messageList) {
                    MessageQueue queue = messageQueueMap.get(queueId);
                    producer.send(message, queue);
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("batch insert error ", e);
        }

        return true;
    }


    protected void initProducer() {
        if (producer == null) {
            synchronized (this) {
                if (producer == null) {
                    destroy();
                    producer = new DefaultMQProducer(groupName + "producer", true, null);
                    try {
                        if (this.namesrvAddr == null || "".equals(this.namesrvAddr)) {
                            throw new RuntimeException("namesrvAddr can not be null.");
                        }

                        producer.setNamesrvAddr(this.namesrvAddr);
                        producer.start();
                    } catch (Exception e) {
                        setInitSuccess(false);
                        throw new RuntimeException("create producer failed," + topic + ",msg=" + e.getMessage(), e);
                    }
                }
            }

        }
    }

    public void destroyProduce() {
        if (producer != null) {
            try {
                producer.shutdown();
                producer = null;
            } catch (Throwable t) {
                if (LOG.isWarnEnabled()) {
                    LOG.warn(t.getMessage(), t);
                }
            }
        }
    }


    @Override
    public void destroy() {
        super.destroy();
        destroyProduce();
    }

    @Override
    public String getShuffleTopicFieldName() {
        return "topic";
    }

    @Override
    protected void createTopicIfNotExist(int splitNum) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setNamesrvAddr(this.getNamesrvAddr());
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {
            defaultMQAdminExt.start();
            TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
            for (String topic : topicList.getTopicList()) {
                if (topic.equals(this.topic)) {
                    return;
                }
            }


            defaultMQAdminExt.createTopic(MixAll.AUTO_CREATE_TOPIC_KEY_TOPIC, topic, splitNum, 1);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("create topic error " + topic, e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }


    @Override
    public List<ISplit> getSplitList() {
        initProducer();
        List<ISplit> messageQueues = new ArrayList<>();
        try {

            if (messageQueues == null || messageQueues.size() == 0) {
                List<MessageQueue> metaqQueueSet = producer.fetchPublishMessageQueues(topic);
                List<ISplit> queueList = new ArrayList<>();
                for (MessageQueue queue : metaqQueueSet) {
                    RocketMQMessageQueue rocketMQMessageQueue = new RocketMQMessageQueue(queue);
                    queueList.add(rocketMQMessageQueue);

                }
                Collections.sort(queueList);
                messageQueues = queueList;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return messageQueues;
    }

    @Override
    public int getSplitNum() {
        List<ISplit> splits = getSplitList();
        if (splits == null || splits.size() == 0) {
            return 0;
        }
        Set<Integer> splitNames = new HashSet<>();
        for (ISplit split : splits) {
            MessageQueue messageQueue = (MessageQueue) split.getQueue();
            splitNames.add(messageQueue.getQueueId());
        }
        return splitNames.size();
    }


    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public List<DefaultMQPushConsumer> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<DefaultMQPushConsumer> consumers) {
        this.consumers = consumers;
    }

    public DefaultMQProducer getProducer() {
        return producer;
    }

    public void setProducer(DefaultMQProducer producer) {
        this.producer = producer;
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

}
