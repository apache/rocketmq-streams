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
package org.apache.rocketmq.streams.metaq.sink;

import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.taobao.metaq.client.MetaProducer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.metaq.queue.MetaqMessageQueue;

public class MetaqSink extends AbstractSupportShuffleSink {

    protected static final Log LOG = LogFactory.getLog(MetaqSink.class);
    private static final String PREFIX = "dipper.upgrade.channel.metaq.envkey";
    @ENVDependence
    protected String tags = "*";                                       // 每个消息队列都会有一个线程
    protected transient MetaProducer producer;
    protected String topic;

    public MetaqSink() {
    }

    public MetaqSink(String topic, String tags) {
        setTopic(topic);
        setTags(tags);
    }

    public MetaqSink(String topic) {
        setTopic(topic);
    }

    public static void main(String[] args) throws InterruptedException {
        String topic = "TOPIC_DIPPER_SYSTEM_MSG_9";
        MetaqSink metaqSink = new MetaqSink(topic);
        metaqSink.setSplitNum(5);
        metaqSink.init();
        System.out.println(metaqSink.getSplitNum());
        // System.out.println(metaqSink.getSplitList());
        // MetaqSource metaqSource=new MetaqSource(topic,"fds");
        // metaqSource.init();
        // metaqSource.start(new IStreamOperator() {
        //     @Override
        //     public Object doMessage(IMessage message, AbstractContext context) {
        //         System.out.println(message.getHeader().getQueueId());
        //         return null;
        //     }
        // });
        Thread.sleep(100000000);
        //DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        //defaultMQAdminExt.setVipChannelEnabled(false);
        //defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        //
        //try {
        //    defaultMQAdminExt.start();
        //
        //    TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);
        //    String json = topicRouteData.toJson(true);
        //    System.out.printf("%s%n", json);
        //} catch (Exception e) {
        //    e.printStackTrace();
        //} finally {
        //    defaultMQAdminExt.shutdown();
        //}
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

    public String getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = tags;
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
                ISplit<?, ?> channelQueue = getSplit(msg);
                String queueId = defaultQueueId;
                if (channelQueue != null) {
                    queueId = channelQueue.getQueueId();
                    MetaqMessageQueue metaqMessageQueue = (MetaqMessageQueue) channelQueue;
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

                    producer = new MetaProducer(UUID.randomUUID() + "producer");
                    producer.setSendMessageWithVIPChannel(false);//add by 明亮
                    producer.setInstanceName(UUID.randomUUID().toString());
                    try {
                        producer.start();
                    } catch (Exception e) {
                        throw new RuntimeException("创建队列失败," + topic + ",msg=" + e.getMessage(), e);
                    }
                }
            }

        }

    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public String getShuffleTopicFieldName() {
        return "topic";
    }

    @Override
    protected void createTopicIfNotExist(int splitNum) {

        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(10000);
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        try {

            defaultMQAdminExt.start();
            TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
            for (String topic : topicList.getTopicList()) {
                if (topic.equals(this.topic)) {
                    return;
                }
            }
            defaultMQAdminExt.createTopic(MixAll.DEFAULT_TOPIC, topic, splitNum, 1);

        } catch (Exception e) {
            throw new RuntimeException("create topic error " + topic, e);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }

    @Override
    public List<ISplit<?, ?>> getSplitList() {
        initProducer();
        List<ISplit<?, ?>> messageQueues = new ArrayList<>();
        try {

            List<MessageQueue> metaqQueueSet = producer.fetchPublishMessageQueues(topic);
            List<ISplit<?, ?>> queueList = new ArrayList<>();
            for (MessageQueue queue : metaqQueueSet) {
                MetaqMessageQueue metaqMessageQueue = new MetaqMessageQueue(queue);
                queueList.add(metaqMessageQueue);

            }
            queueList.sort((Comparator<ISplit>) Comparable::compareTo);
            messageQueues = queueList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return messageQueues;
    }

    @Override
    public int getSplitNum() {
        List<ISplit<?, ?>> splits = getSplitList();
        if (splits == null || splits.size() == 0) {
            return 0;
        }
        Set<Integer> splitNames = new HashSet<>();
        for (ISplit<?, ?> split : splits) {
            MessageQueue messageQueue = (MessageQueue) split.getQueue();
            splitNames.add(messageQueue.getQueueId());
        }
        return splitNames.size();
    }

}
