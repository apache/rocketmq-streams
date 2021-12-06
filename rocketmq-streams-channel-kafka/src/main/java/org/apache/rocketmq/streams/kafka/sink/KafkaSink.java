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
package org.apache.rocketmq.streams.kafka.sink;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.kafka.KafkaSplit;
import org.apache.rocketmq.streams.kafka.source.KafkaSource;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;

public class KafkaSink extends AbstractSupportShuffleSink {
    private static final Log LOG = LogFactory.getLog(KafkaSource.class);

    private static final String PREFIX = "dipper.upgrade.channel.kafak.envkey";

    private transient List<KafkaConsumer<String, String>> consumers = new ArrayList<>();
    private transient KafkaProducer kafkaProducer;
    private transient Properties props;

    private static int maxPollRecords = 100;
    private volatile transient boolean stop = false;
    protected String topic;
    @ENVDependence
    private String endpoint;
    private int sessionTimeout = 30000;
    private transient ExecutorService executorService = null;
    private transient ConcurrentLinkedQueue<ConsumerRecord<String, String>> itemQueue = new ConcurrentLinkedQueue<ConsumerRecord<String, String>>();

    public KafkaSink() {}

    public KafkaSink(String endpoint, String topic) {
        this.endpoint = endpoint;
        this.topic = topic;
    }

    @Override
    protected boolean initConfigurable() {
        Properties props = new Properties();
        props.put("bootstrap.servers", endpoint);
        props.put("enable.auto.commit", false);
        props.put("acks", "1");  //针对kafka producer可以做一些参数优化
        props.put("linger.ms", 1);
        props.put("batch.size", 16384);
        props.put("session.timeout.ms", String.valueOf(sessionTimeout));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.props = props;

        kafkaProducer = new KafkaProducer<String, String>(props);

        return true;
    }

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        if (messages == null) {
            return true;
        }
        for (IMessage message : messages) {
            putMessage2Mq(message);
        }
        return true;
    }

    protected void destroyProducer() {
        if (kafkaProducer != null) {
            try {
                kafkaProducer.close();
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
        stop = true;
        destroyProducer();
    }

    @Override
    protected void createTopicIfNotExist(int splitNum) {
        AdminClient adminClient = null;
        try {

            Properties properties = new Properties();
            properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                endpoint);

            adminClient = AdminClient.create(properties);
            ListTopicsResult result = adminClient.listTopics();
            Set<String> topics = result.names().get();
            if (topics.contains(topic)) {
                return;
            }
            NewTopic newTopic = new NewTopic(topic, splitNum, (short)1);
            adminClient.createTopics(Arrays.asList(newTopic));

            LOG.info("创建主题成功：" + topic);
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (adminClient != null) {
                adminClient.close();
            }
        }
    }

    @Override
    public String getShuffleTopicFieldName() {
        return "topic";
    }

    @Override
    public List<ISplit> getSplitList() {
        List<PartitionInfo> partitionInfos = kafkaProducer.partitionsFor(topic);
        List<ISplit> splits = new ArrayList<>();
        for (PartitionInfo partitionInfo : partitionInfos) {
            splits.add(new KafkaSplit(partitionInfo));
        }
        return splits;
    }

    protected boolean putMessage2Mq(IMessage fieldName2Value) {
        try {

            LOG.info(String.format("topic=%s, record=%s", topic, fieldName2Value.getMessageValue().toString()));
            ProducerRecord<String, String> records =
                new ProducerRecord<String, String>(topic, fieldName2Value.getMessageValue().toString());
            kafkaProducer.send(records, new Callback() {

                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        //                    	LOG.error("send kafka message error!topic=" + topic, e);
                    } else {
                        //                    	LOG.info(String.format("send success topic=%s, record=%s", topic, jsonObject.toJSONString()));
                    }
                }
            });
        } catch (Exception e) {
            LOG.error("send message error:" + fieldName2Value.getMessageValue().toString(), e);
            return false;
        }
        return true;
    }

    @Override
    public int getSplitNum() {
        return getSplitList().size();
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

    public int getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

}
