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
package org.apache.rocketmq.streams.client.source;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import java.util.Properties;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.channel.impl.CollectionSource;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemoryCache;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.db.source.CycleDynamicMultipleDBScanSource;
import org.apache.rocketmq.streams.db.source.DynamicMultipleDBScanSource;
import org.apache.rocketmq.streams.connectors.source.filter.CycleSchedule;
import org.apache.rocketmq.streams.kafka.source.KafkaSource;
import org.apache.rocketmq.streams.mqtt.source.PahoSource;
import org.apache.rocketmq.streams.rocketmq.source.RocketMQSource;
import org.apache.rocketmq.streams.sls.source.SLSSource;

public class DataStreamSource {

    private final String namespace;
    private final String jobName;
    private final Properties properties;

    public DataStreamSource(String namespace, String jobName, Properties properties) {
        this.namespace = namespace;
        this.jobName = jobName;
        this.properties = properties;
    }

    public DataStream fromArray(Object[] o) {
        MemoryCache cache = new MemoryCache(o);
        return fromMemory(cache, o instanceof JSONObject[]);
    }

    public DataStream fromMemory(MemoryCache memoryCache, boolean isJson) {
        MemorySource memorySource = new MemorySource();
        memorySource.setMemoryCache(memoryCache);
        memorySource.setJsonData(isJson);
        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.addConfigurables(memoryCache);
        rootPipelineBuilder.setSource(memorySource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromFile(String filePath) {
        return fromFile(filePath, true);
    }

    public DataStream fromCSVFile(String filePath) {
        FileSource fileChannel = new FileSource(filePath);
        fileChannel.setCSV(true);
        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(fileChannel);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromFile(String filePath, Boolean isJsonData) {
        FileSource fileChannel = new FileSource(filePath);
        fileChannel.setJsonData(isJsonData);
        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(fileChannel);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromSls(String endPoint, String project, String logStore, String ak, String sk, String groupName) {
        SLSSource slsSource = new SLSSource(endPoint, project, logStore, ak, sk, groupName);
        slsSource.setJsonData(true);
        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(slsSource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromRocketmq(String topic, String groupName, String nameServer) {
        return fromRocketmq(topic, groupName, false, nameServer);
    }

    public DataStream fromRocketmq(String topic, String groupName, boolean isJson, String nameServer) {
        return fromRocketmq(topic, groupName, "*", isJson, nameServer);
    }

    public DataStream fromRocketmq(String topic, String groupName, String tags, boolean isJson, String nameServer) {
        return fromRocketmq(topic, groupName, tags, isJson, nameServer, false);
    }

    public DataStream fromRocketmq(String topic, String groupName, String tags, boolean isJson, String nameServer, boolean isMessageListenerConcurrently) {

        RocketMQSource rocketMQSource = new RocketMQSource();
        rocketMQSource.setTopic(topic);
        rocketMQSource.setTags(tags);
        rocketMQSource.setGroupName(groupName);
        rocketMQSource.setJsonData(isJson);
        rocketMQSource.setNamesrvAddr(nameServer);
        rocketMQSource.setMessageListenerConcurrently(isMessageListenerConcurrently);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(rocketMQSource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromMultipleDB(String url, String userName, String password, String tablePattern) {
        DynamicMultipleDBScanSource source = new DynamicMultipleDBScanSource();
        source.setUrl(url);
        source.setUserName(userName);
        source.setPassword(password);
        source.setBatchSize(10);
        source.setLogicTableName(tablePattern);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(source);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromCycleSource(String url, String userName, String password, String tablePattern, CycleSchedule.Cycle cycle, int balanceSec) {
        CycleDynamicMultipleDBScanSource source = new CycleDynamicMultipleDBScanSource(cycle);
        source.setUrl(url);
        source.setUserName(userName);
        source.setPassword(password);
        source.setBatchSize(10);
        source.setLogicTableName(tablePattern);
        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(source);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromCollection(JSONObject... elements) {
        CollectionSource source = new CollectionSource();
        source.addAll(elements);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(source);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromMqtt(String url, String clientId, String topic) {
        PahoSource mqttSource = new PahoSource(url, clientId, topic);
        mqttSource.setJsonData(true);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(mqttSource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromMqtt(String url, String clientId, String topic, String username, String password) {
        PahoSource mqttSource = new PahoSource(url, clientId, topic, username, password);
        mqttSource.setJsonData(true);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(mqttSource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromMqttForJsonArray(String url, String clientId, String topic, String username, String password) {
        PahoSource mqttSource = new PahoSource(url, clientId, topic, username, password);
        mqttSource.setJsonData(true);
        mqttSource.setMsgIsJsonArray(true);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(mqttSource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromMqtt(String url, String clientId, String topic, String username, String password, Boolean cleanSession, Integer connectionTimeout, Integer aliveInterval, Boolean automaticReconnect) {
        return fromMqtt(url, clientId, topic, username, password, cleanSession, connectionTimeout, aliveInterval, automaticReconnect, true);
    }

    public DataStream fromMqtt(String url, String clientId, String topic, String username, String password, Boolean cleanSession, Integer connectionTimeout, Integer aliveInterval, Boolean automaticReconnect, Boolean jsonData) {
        PahoSource mqttSource = new PahoSource(url, clientId, topic, username, password, cleanSession, connectionTimeout, aliveInterval, automaticReconnect);
        mqttSource.setJsonData(jsonData);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(mqttSource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromMqtt(String url, String clientId, String topic, String username, String password, Boolean jsonData) {
        PahoSource mqttSource = new PahoSource(url, clientId, topic, username, password);
        mqttSource.setJsonData(jsonData);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(mqttSource);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream fromKafka(String endpoint, String topic, String groupName) {
        return fromKafka(endpoint, topic, groupName, true);
    }

    public DataStream fromKafka(String endpoint, String topic, String groupName, Boolean isJson) {
        return fromKafka(endpoint, topic, groupName, isJson, 1);
    }

    public DataStream fromKafka(String endpoint, String topic, String groupName, Boolean isJson, int maxThread) {
        KafkaSource kafkaChannel = new KafkaSource();
        kafkaChannel.setBootstrapServers(endpoint);
        kafkaChannel.setTopic(topic);
        kafkaChannel.setGroupName(groupName);
        kafkaChannel.setJsonData(isJson);
        kafkaChannel.setMaxThread(maxThread);

        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(kafkaChannel);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

    public DataStream from(ISource<?> source) {
        PipelineBuilder rootPipelineBuilder = new PipelineBuilder(namespace, jobName, properties);
        rootPipelineBuilder.setSource(source);
        return new DataStream(rootPipelineBuilder, Sets.newHashSet(), null);
    }

}
