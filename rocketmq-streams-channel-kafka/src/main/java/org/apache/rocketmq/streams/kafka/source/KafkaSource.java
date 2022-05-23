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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportShuffleSource;
import org.apache.rocketmq.streams.common.context.Message;

public class KafkaSource extends AbstractSupportShuffleSource {

    private static final Log LOG = LogFactory.getLog(KafkaSource.class);

    private transient Properties props;
    private String bootstrapServers;
    private transient KafkaConsumer<String, String> consumer;

    private int maxPollRecords = 100;
    private volatile transient boolean stop = false;
    private int sessionTimeout = 30000;

    public KafkaSource() {
    }

    public KafkaSource(String bootstrapServers, String topic, String groupName) {
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupName = groupName;
    }

    @Override protected boolean initConfigurable() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
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
        props.put("key.serializer.encoding", getEncoding());
        props.put("value.serializer.encoding", getEncoding());
        this.props = props;
        return super.initConfigurable();
    }

    @Override protected boolean startSource() {
        try {
            //kafka多线程消费会报“KafkaConsumer is not safe for multi-threaded access”，所以改成单线程拉取，多线程处理
            if (this.consumer == null) {
                this.consumer = new KafkaConsumer<>(props);
                this.consumer.subscribe(Lists.newArrayList(topic));
            }
            WorkerFunc workerFunc = new WorkerFunc();
            ExecutorService executorService = new ThreadPoolExecutor(getMaxThread(), getMaxThread(), 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1000));
            executorService.execute(workerFunc);
        } catch (Exception e) {
            setInitSuccess(false);
            LOG.error(e.getMessage(), e);
            destroy();
            throw new RuntimeException(" start kafka channel error " + topic);
        }
        return true;
    }

    protected class WorkerFunc implements Runnable {

        @Override public void run() {
            long lastUpgrade = System.currentTimeMillis();
            List<PartitionInfo> oldPartitionInfos = consumer.partitionsFor(topic);
            while (!stop) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(1000);
                    List<PartitionInfo> newPartitionInfos = consumer.partitionsFor(topic);
                    messageQueueChanged(oldPartitionInfos, newPartitionInfos);
                    Set<String> queueIds = new HashSet<>();
                    for (ConsumerRecord<String, String> record : records) {
                        try {
                            queueIds.add(record.partition() + "");
                            Map<String, Object> parameters = new HashMap<>();
                            if (getHeaderFieldNames() != null) {
                                parameters.put("messageKey", record.key());
                                parameters.put("topic", record.topic());
                                parameters.put("partition", record.partition());
                                parameters.put("offset", record.offset());
                                parameters.put("timestamp", record.timestamp());
                            }
                            JSONObject msg = create(record.value(), parameters);
                            Message message = createMessage(msg, record.partition() + "", record.offset() + "", false);
                            message.getHeader().setOffsetIsLong(true);
                            executeMessage(message);
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

    @Override public boolean supportNewSplitFind() {
        return true;
    }

    @Override public boolean supportRemoveSplitFind() {
        return true;
    }

    @Override public boolean supportOffsetRest() {
        return false;
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    protected void destroyConsumer() {
        if (this.consumer != null) {
            this.consumer.close();
        }
    }

    @Override public void destroy() {
        super.destroy();
        stop = true;
        destroyConsumer();
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public boolean isStop() {
        return stop;
    }

    public void setStop(boolean stop) {
        this.stop = stop;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @Override public void setMaxFetchLogGroupSize(int size) {
        super.setMaxFetchLogGroupSize(size);
        maxPollRecords = size;
    }

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public KafkaConsumer<String, String> getConsumer() {
        return consumer;
    }

    public void setConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
    }
}
