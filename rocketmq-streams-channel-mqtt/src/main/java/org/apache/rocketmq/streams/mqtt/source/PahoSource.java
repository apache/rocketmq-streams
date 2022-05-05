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

import com.alibaba.fastjson.JSONObject;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallback;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PahoSource extends AbstractSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(PahoSource.class);

    private String url;
    private String clientId;
    private String topic;
    private String username;
    private String password;
    private Boolean cleanSession;
    private Integer connectionTimeout;
    private Integer aliveInterval;
    private Boolean automaticReconnect;

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

    private transient MqttClient client;
    protected transient AtomicLong offsetGenerator;

    @Override protected boolean startSource() {
        try {
            this.client = new MqttClient(url, clientId, new MemoryPersistence());
            this.offsetGenerator = new AtomicLong(System.currentTimeMillis());
            this.client.setCallback(new MqttCallback() {

                @Override public void connectionLost(Throwable throwable) {
                    LOGGER.info("Reconnecting to broker: " + url);
                    while (true) {
                        MqttConnectOptions connOpts = new MqttConnectOptions();
                        if (username != null && password != null) {
                            connOpts.setUserName(username);
                            connOpts.setPassword(password.toCharArray());
                        }
                        if (cleanSession == null) {
                            connOpts.setCleanSession(true);
                        } else {
                            connOpts.setCleanSession(cleanSession);
                        }

                        if (connectionTimeout == null) {
                            connOpts.setConnectionTimeout(10);
                        } else {
                            connOpts.setConnectionTimeout(connectionTimeout);
                        }
                        if (aliveInterval == null) {
                            connOpts.setKeepAliveInterval(60);
                        } else {
                            connOpts.setKeepAliveInterval(aliveInterval);
                        }
                        if (automaticReconnect == null) {
                            connOpts.setAutomaticReconnect(true);
                        } else {
                            connOpts.setAutomaticReconnect(automaticReconnect);
                        }

                        try {
                            if (!client.isConnected()) {
                                client.connect(connOpts);
                                LOGGER.info("Reconnecting success");
                            }
                            client.subscribe(topic);
                            break;
                        } catch (MqttException e) {
                            try {
                                LOGGER.error("Reconnecting err: " + e.getMessage());
                                e.printStackTrace();
                                Thread.sleep(10000);
                            } catch (InterruptedException ex) {
                                ex.printStackTrace();
                            }
                        }
                    }
                }

                @Override public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {
                    JSONObject msg = create(new String(mqttMessage.getPayload(), StandardCharsets.UTF_8));
                    msg.put("__topic", s);
                    doReceiveMessage(msg, false, RuntimeUtil.getDipperInstanceId(), offsetGenerator.incrementAndGet() + "");
                }

                @Override public void deliveryComplete(IMqttDeliveryToken token) {
                    LOGGER.info("deliveryComplete---------" + token.isComplete());
                }
            });

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

            LOGGER.info("Connecting to broker: " + url);
            if (!this.client.isConnected()) {
                this.client.connect(connOpts);
                LOGGER.info("Connected");
            }
            this.client.subscribe(topic);
            return true;
        } catch (MqttException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override public void destroy() {
        super.destroy();
        try {
            if (this.client != null && this.client.isConnected()) {
                this.client.disconnect();
                this.client.close();
            }
            super.destroy();
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        return false;
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

    @Override public String getTopic() {
        return topic;
    }

    @Override public void setTopic(String topic) {
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

}
