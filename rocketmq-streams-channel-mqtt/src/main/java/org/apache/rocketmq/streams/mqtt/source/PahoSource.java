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
package org.apache.rocketmq.streams.mqtt.source;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.split.CommonSplit;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.mqtt.factory.MessageProcess;
import org.apache.rocketmq.streams.mqtt.factory.MqttClientFactory;
import org.apache.rocketmq.streams.mqtt.factory.MqttConnection;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PahoSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PahoSource.class);
    protected transient String clientKey;
    protected transient AtomicLong offsetGenerator;
    private String url;
    private String clientId;
    private String topic;
    private String username;
    private String password;
    private Boolean cleanSession;
    private Integer connectionTimeout;
    private Integer aliveInterval;
    private Boolean automaticReconnect;
    private transient MqttClient client;
    // 记录上次持久化 check point 的时间
    private transient long mLastCheckTime = 0;

    public PahoSource() {
    }

    public PahoSource(String url, String clientId) {
        this(url, clientId, null);
    }

    public PahoSource(String url, String clientId, String topic) {
        this(url, clientId, topic, null, null);
    }

    public PahoSource(String url, String clientId, String topic, String username, String password) {
        this(url, clientId, topic, username, password, true, 10, 60, true);
    }

    public PahoSource(String url, String clientId, String topic, String username, String password, Boolean cleanSession, Integer connectionTimeout, Integer aliveInterval, Boolean automaticReconnect) {
        this.url = url;
        this.clientId = clientId;
        this.topic = topic;
        this.username = username;
        this.password = password;
        this.cleanSession = cleanSession;
        this.connectionTimeout = connectionTimeout;
        this.aliveInterval = aliveInterval;
        this.automaticReconnect = automaticReconnect;
    }

    @Override
    protected boolean startSource() {
        try {
            this.offsetGenerator = new AtomicLong(System.currentTimeMillis());
            MqttConnection connection = new MqttConnection(this.url, this.topic, this.username, this.password, this.clientId, getOptions());
            connection.setAutoConnect(true);
            connection.setMessageProcess(new SourceMessageProcess());
            clientKey = connection.getClientKey();
            MqttClient client = MqttClientFactory.getClient(connection);
            if (null == client) {
                LOGGER.error("PahoSource startSource error,MQTT Client IS Empty!");
                return false;
            }
            this.client = client;
            return true;
        } catch (Exception ex) {
            LOGGER.error("PahoSource startSource error,broker:{},ex:{}", url, ExceptionUtils.getStackTrace(ex));
        }
        return false;
    }

    public MqttConnectOptions getOptions() {
        MqttConnectOptions connOpts = new MqttConnectOptions();
        if (username != null && password != null) {
            connOpts.setUserName(username);
            connOpts.setPassword(password.toCharArray());
        }
        if (this.cleanSession == null) {
            connOpts.setCleanSession(true);
        } else {
            connOpts.setCleanSession(this.cleanSession);
        }
        if (this.connectionTimeout == null) {
            connOpts.setConnectionTimeout(10);
        } else {
            connOpts.setConnectionTimeout(this.connectionTimeout);
        }
        if (this.aliveInterval == null) {
            connOpts.setKeepAliveInterval(60);
        } else {
            connOpts.setKeepAliveInterval(this.aliveInterval);
        }
        if (this.automaticReconnect == null) {
            connOpts.setAutomaticReconnect(true);
        } else {
            connOpts.setAutomaticReconnect(this.automaticReconnect);
        }
        return connOpts;
    }

    @Override
    public void destroySource() {
        MqttClientFactory.removeClient(clientKey);
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    @Override
    public String getTopic() {
        return topic;
    }

    @Override
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @Override
    public List<ISplit<?, ?>> fetchAllSplits() {
        ISplit<?, ?> split = new CommonSplit("1");
        List<ISplit<?, ?>> splits = new ArrayList<>();
        splits.add(split);
        return splits;
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

    public Boolean getCleanSession() {
        return cleanSession;
    }

    public void setCleanSession(Boolean cleanSession) {
        this.cleanSession = cleanSession;
    }

    public Integer getConnectionTimeout() {
        return connectionTimeout;
    }

    public void setConnectionTimeout(Integer connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }

    public Integer getAliveInterval() {
        return aliveInterval;
    }

    public void setAliveInterval(Integer aliveInterval) {
        this.aliveInterval = aliveInterval;
    }

    public Boolean getAutomaticReconnect() {
        return automaticReconnect;
    }

    public void setAutomaticReconnect(Boolean automaticReconnect) {
        this.automaticReconnect = automaticReconnect;
    }

    /**
     * 消息回调
     */
    class SourceMessageProcess implements MessageProcess {
        @Override
        public void process(String topic, MqttMessage message) {
            String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
            Map<String, Object> additionValues = new HashMap<>();
            additionValues.put("__topic", topic);
            String queueId = offsetGenerator.incrementAndGet() + "";
            doReceiveMessage(msg, false, RuntimeUtil.getDipperInstanceId(), queueId, additionValues);

            long curTime = System.currentTimeMillis();
            //每200ms调用一次
            // 每隔 60 秒，写一次 check point 到服务端，如果 60 秒内，worker crash，
            // 新启动的 worker 会从上一个 checkpoint 其消费数据，有可能有重复数据
            if (curTime - mLastCheckTime > getCheckpointTime()) {
                mLastCheckTime = curTime;
                sendCheckpoint(String.valueOf(queueId));
            }
        }
    }

}