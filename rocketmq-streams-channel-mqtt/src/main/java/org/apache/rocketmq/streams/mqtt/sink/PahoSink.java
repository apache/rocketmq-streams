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
package org.apache.rocketmq.streams.mqtt.sink;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class PahoSink extends AbstractSink {

    private String broker;
    private String clientId;
    private String topic;
    private int qos;
    private String username;
    private String password;

    private transient MqttClient client;

    public PahoSink() {
    }

    public PahoSink(String broker, String clientId, String topic) {
        this(broker, clientId, topic, null, null);
    }

    public PahoSink(String broker, String clientId, String topic, String username, String password) {
        this(broker, clientId, topic, null, null, 2);
    }

    public PahoSink(String broker, String clientId, String topic, String username, String password, int qos) {
        this.broker = broker;
        this.clientId = clientId;
        this.topic = topic;
        this.username = username;
        this.password = password;
        this.qos = qos;
    }

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        try {
            if (this.client == null) {
                this.client = new MqttClient(broker, clientId, new MemoryPersistence());
            }
            if (!this.client.isConnected()) {
                MqttConnectOptions connOpts = new MqttConnectOptions();
                connOpts.setCleanSession(true);
                if (this.username != null && this.password != null) {
                    connOpts.setUserName(this.username);
                    connOpts.setPassword(this.password.toCharArray());
                }
                System.out.println("Connecting to broker: " + broker);
                this.client.connect(connOpts);
                System.out.println("Connected");
            }

            for (IMessage msg : messages) {
                String messageString = "";
                if (msg.isJsonMessage()) {
                    messageString = JSONObject.toJSONString(msg.getMessageValue());
                } else {
                    messageString = msg.getMessageValue().toString();
                }
                MqttMessage message = new MqttMessage(messageString.getBytes());
                message.setQos(qos);
                this.client.publish(topic, message);
            }
            return true;
        } catch (MqttException e) {
            System.err.println("reason " + e.getReasonCode());
            System.err.println("msg " + e.getMessage());
            System.err.println("loc " + e.getLocalizedMessage());
            System.err.println("cause " + e.getCause());
            System.err.println("exception " + e);
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public void destroy() {
        super.destroy();
        try {
            if (this.client != null) {
                this.client.disconnect();
                this.client.close();
            }
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
}
