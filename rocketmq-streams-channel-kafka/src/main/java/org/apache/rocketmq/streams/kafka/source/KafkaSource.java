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
package org.apache.rocketmq.streams.kafka.source;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;

import org.apache.kafka.common.PartitionInfo;
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportShuffleSource;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaSource extends AbstractSupportShuffleSource {

    private static final Log LOG = LogFactory.getLog(KafkaSource.class);

    private static final String PREFIX = "dipper.upgrade.channel.kafak.envkey";

    private transient List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
    private transient KafkaProducer kafkaProducer;
    private transient Properties props;

    private static int maxPollRecords = 100;
    private volatile transient boolean stop = false;

    @ENVDependence
    private String endpoint;
    private int sessionTimeout = 30000;
    private transient ExecutorService executorService = null;
    private transient ConcurrentLinkedQueue<ConsumerRecord<String, String>> itemQueue =
        new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();
    //private int QUEUE_MAX_SIZE = 5000000;

    public KafkaSource() {
    }

    public KafkaSource(String endpoint, String topic, String groupName) {
        this.endpoint = endpoint;
        this.topic = topic;
        this.groupName = groupName;
    }

    @Override
    protected boolean initConfigurable() {
        Properties props = new Properties();
        props.put("bootstrap.servers", endpoint);
        props.put("group.id", groupName);//这里是GroupA或者GroupB
        props.put("enable.auto.commit", false);
        props.put("acks", "1");  //针对kafka producer可以做一些参数优化
        props.put("linger.ms", 1);
        props.put("batch.size", 16384);
        props.put("auto.commit.interval.ms", String.valueOf(checkpointTime));
        props.put("session.timeout.ms", String.valueOf(sessionTimeout));
        props.put("max.poll.records", String.valueOf(getMaxFetchLogGroupSize()));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("key.serializer.encoding",getEncoding());
        props.put("value.serializer.encoding",getEncoding());
        this.props = props;

        kafkaProducer = new KafkaProducer<String, String>(props);

        return true;
    }

    @Override
    protected boolean startSource() {
        try {
            //kafka多线程消费会报“KafkaConsumer is not safe for multi-threaded access”，所以改成单线程拉取，多线程处理
            WorkerFunc workerFunc = new WorkerFunc();
            ExecutorService executorService = new ThreadPoolExecutor(getMaxThread(), getMaxThread(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1000));
            executorService.execute(workerFunc);
            //处理

        } catch (Exception e) {
            setInitSuccess(false);
            LOG.error(e.getMessage(), e);
            destroy();
            throw new RuntimeException(" start kafka channel error " + topic);
        }
        return true;
    }

    protected class WorkerFunc implements Runnable {
        @Override
        public void run() {
            List<String> topicList = new ArrayList<>();
            topicList.add(topic);
            KafkaConsumer<String,String> consumer = new KafkaConsumer(props);
            consumer.subscribe(topicList);
            long lastUpgrade = System.currentTimeMillis();
            List<PartitionInfo> oldPartitionInfos = consumer.partitionsFor(topic);
            while (true && !stop) {
                try {

                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    List<PartitionInfo> newPartitionInfos = consumer.partitionsFor(topic);
                    messageQueueChanged(oldPartitionInfos, newPartitionInfos);
                    Set<String> queueIds = new HashSet<>();
                    for (ConsumerRecord<String,String> record : records) {
                        try {
                            //		                    LOG.info(String.format("recordValue=%s", record.value()));
                            queueIds.add(record.partition() + "");
                            if(getHeaderFieldNames()!=null){
                                Map<String,Object> parameters=new HashMap<>();
                                parameters.put("messageKey",record.key());
                                parameters.put("topic",record.topic());
                                parameters.put("partition",record.partition());
                                parameters.put("offset",record.offset());
                                parameters.put("timestamp",record.timestamp());
                            }
                            JSONObject msg = create(record.value(),null);
                            Message message = createMessage(msg, record.partition() + "", record.offset() + "", false);
                            message.getHeader().setOffsetIsLong(true);
                            executeMessage(message);
                            doReceiveMessage(msg, false, record.partition() + "", record.offset() + "");
                        } catch (Exception e) {
                            LOG.error(e.getMessage(), e);
                        }
                    }
                    if ((System.currentTimeMillis() - lastUpgrade) > checkpointTime) {
                        sendCheckpoint(queueIds);
                        consumer.commitAsync();
                        lastUpgrade = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }

            }
        }
    }

    public void messageQueueChanged(List<PartitionInfo> old, List<PartitionInfo> newPartitions) {
        Set<String> queueIds = new HashSet<>();
        Set<String> mqAll = new HashSet<>();
        Set<String> mqDivided = new HashSet<>();
        for (PartitionInfo partitionInfo : old) {
            mqAll.add(partitionInfo.partition() + "");
        }
        for (PartitionInfo partitionInfo : newPartitions) {
            mqDivided.add(partitionInfo.partition() + "");
        }
        for (String messageQueue : mqAll) {
            if (!mqDivided.contains(messageQueue)) {
                //ProcessQueue pq = this.processQueueTable.remove(messageQueue);
                //if (pq != null) {
                //    pq.setDropped(true);
                //    log.info("doRebalance, {}, truncateMessageQueueNotMyTopic remove unnecessary mq, {}", consumerGroup, messageQueue);
                //}
                queueIds.add(messageQueue);
            }
        }
        Set<String> newQueueIds = new HashSet<>();
        for (String messageQueue : mqDivided) {
            if (!mqAll.contains(messageQueue)) {
                newQueueIds.add(messageQueue + "");
            }
        }
        removeSplit(queueIds);
        addNewSplit(newQueueIds);

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

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    protected void destroyConsumer() {
        if (consumers != null && consumers.size() > 0) {
            for (KafkaConsumer consumer : consumers) {
                if (consumer != null) {
                    consumer.close();
                }
            }

        }
    }

    @Override
    public void destroy() {
        super.destroy();
        stop = true;
        destroyConsumer();
    }

    public static int getMaxPollRecords() {
        return maxPollRecords;
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    @Override
    public void setMaxFetchLogGroupSize(int size) {
        super.setMaxFetchLogGroupSize(size);
        maxPollRecords = size;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

}
