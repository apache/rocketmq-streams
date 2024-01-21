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

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.rocketmq.streams.common.channel.source.AbstractPushSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.kafka.KafkaSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSource extends AbstractPushSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

    private transient Properties props;
    private String bootstrapServers;

    private String userName;

    private String password;
    private String autoOffsetReset = "earliest";

    private transient KafkaConsumer<String, String> consumer;

    private int maxPollRecords = 100;
    private volatile transient boolean stop = false;
    private volatile transient boolean isFinished = false;
    private int sessionTimeout = 30000;
    private transient ExecutorService executorService;

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
        props.put("auto.offset.reset", autoOffsetReset);
        props.put("key.serializer.encoding", getEncoding());
        props.put("value.serializer.encoding", getEncoding());
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        if (this.userName != null && this.password != null) {
            props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required serviceName=\"" + this.getNameSpace() + "_" + this.getName() + "\" username=\"" + userName + "\" password=\"" + password + "\";");
        }
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
            if (executorService == null) {
                executorService = ThreadPoolFactory.createFixedThreadPool(getMaxThread(), KafkaSource.class.getName() + "-" + getName());
            }
            executorService.execute(workerFunc);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            try {
                destroy();
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException(" start kafka channel error " + topic);
        }
        return true;
    }

    @Override protected boolean hasListenerSplitChanged() {
        return true;
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

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        KafkaConsumer consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Lists.newArrayList(topic));
        try {
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
            List<ISplit<?, ?>> splits = new ArrayList<>();
            for (PartitionInfo partitionInfo : partitionInfos) {
                splits.add(new KafkaSplit(partitionInfo));
            }
            return splits;

        } finally {
            consumer.close();
        }

    }

    protected void destroyConsumer() {
        this.stop = true;

        while (!isFinished) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (this.consumer != null) {
            this.consumer.close();

        }
        this.stop = false;
        this.isFinished = false;
        if (this.executorService != null) {
            this.executorService.shutdown();
            this.executorService = null;
        }
    }

    @Override public void destroySource() {
        stop = true;
        destroyConsumer();
    }

    public int getMaxPollRecords() {
        return maxPollRecords;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
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

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
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
                            doReceiveMessage(record.value(), false, record.partition() + "", record.offset() + "", parameters);
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    }
                    if ((System.currentTimeMillis() - lastUpgrade) > checkpointTime) {
                        sendCheckpoint(queueIds);
                        consumer.commitAsync();
                        lastUpgrade = System.currentTimeMillis();
                    }
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
            isFinished = true;
        }
    }

    public String getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(String autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }
}
