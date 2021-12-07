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
package org.apache.rocketmq.streams.kafka;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.kafka.sink.KafkaSink;
import org.apache.rocketmq.streams.kafka.source.KafkaSource;
import org.junit.Test;

/**
 * 本机搭建kafka服务端参考：https://www.cnblogs.com/BlueSkyyj/p/11425998.html
 **/
public class KafkaChannelTest {
    private static final String END_POINT = "47.94.238.133:31013";
    private static final String TOPIC = "real_time_kafka_topic";
    //数据任务处理后topic:es_index_test,实时策略消息topic:real_time_kafka_topic
    private static final String GROUP_NAME = "test-090";

    @Test
    public void testKafkaReceive() throws InterruptedException {
        ISource channel = createSource();
        channel.start((message, context) -> {
            System.out.println(message.getMessageBody());
            return message;
        });
        while (true) {
            Thread.sleep(1000L);
        }
    }

    @Test
    public void testSendMessageToKafka() throws InterruptedException {
        ISink channel = createSink();
        String data = "{\"destIp\":\"10.252.30.235\",\"protocol\":\"tcp\",\"destPort\":\"10000\",\"sourceIp\":\"10"
            + ".232.1.1\",\"level\":\"high\",\"srcPort\":\"8000\",\"monitor_type\":\"auto\"}";
        //
        String alarmData = "{\"destIp\":\"10.252.30.235\",\"fixMethod\":\"测试策略，不用处理\",\"level\":\"urgent\","
            + "\"modelId\":\"666\",\"alarmTime\":\"2020-03-12 19:35:31\",\"alarmName\":\"完整流程测试策略名称\","
            + "\"srcPort\":\"8000\",\"source\":\"完整数据流串测-es通道-不要删除\",\"protocol\":\"tcp\",\"monitorType\":\"hand\","
            + "\"destPort\":\"10000\",\"sourceIp\":\"10.232.1.1\","
            + "\"fireRules\":[{\"ruleName\":\"pipeline_test_config_namestrategy_filter_stage\","
            + "\"ruleNameSpace\":\"paas_soc_namespace\"}],\"modelDesc\":\"测试策略，不要在意\",\"detail\":{\"destIp\":\"10.252"
            + ".30.235\",\"fixMethod\":\"测试策略，不用处理\",\"level\":\"urgent\",\"modelId\":\"666\","
            + "\"alarmTime\":\"2020-03-12 19:35:31\",\"alarmName\":\"完整流程测试策略名称\",\"srcPort\":\"8000\","
            + "\"source\":\"完整数据流串测-es通道-不要删除\",\"protocol\":\"tcp\",\"monitorType\":\"hand\",\"destPort\":\"10000\","
            + "\"sourceIp\":\"10.232.1.1\","
            + "\"fireRules\":[{\"ruleName\":\"pipeline_test_config_namestrategy_filter_stage\","
            + "\"ruleNameSpace\":\"paas_soc_namespace\"}],\"modelDesc\":\"测试策略，不要在意\",\"monitor_type\":\"auto\"},"
            + "\"monitor_type\":\"auto\"}";
        //for (int i = 0; i < 1; i++) {
        //    JSONObject message = new JSONObject();
        //    message.put("key", "message_key " + i);
        //    message.put("value", 999 + i);
        //    channel.batchAdd(message);
        //}
        channel.batchAdd(new Message(JSONObject.parseObject(alarmData)));
        channel.flush();
        /**
         * 注意：发送之后不能立即退出应用，不然消息可能还没有发出，会发生消息丢失
         */
        Thread.sleep(1000L);
    }

    private ISink createSink() {
        KafkaSink kafkaChannel = new KafkaSink();
        kafkaChannel.setTopic(TOPIC);
        kafkaChannel.setEndpoint(END_POINT);
        kafkaChannel.setNameSpace("com.aliyun.dipper.test");
        kafkaChannel.setConfigureName("kafka_channel");
        kafkaChannel.init();
        return kafkaChannel;
    }

    private ISource createSource() {
        KafkaSource kafkaChannel = new KafkaSource();
        kafkaChannel.setJsonData(false);
        kafkaChannel.setTopic(TOPIC);
        kafkaChannel.setEndpoint(END_POINT);
        kafkaChannel.setGroupName(GROUP_NAME);
        kafkaChannel.setMaxThread(1);
        kafkaChannel.setNameSpace("com.aliyun.dipper.test");
        kafkaChannel.setConfigureName("kafka_channel");
        kafkaChannel.init();
        return kafkaChannel;
    }
}
