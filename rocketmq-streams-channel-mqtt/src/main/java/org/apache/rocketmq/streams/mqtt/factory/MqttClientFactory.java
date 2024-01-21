package org.apache.rocketmq.streams.mqtt.factory;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttCallbackExtended;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author fgm
 * @date 2023/5/7
 * @description MQTT客户端工厂
 */
public class MqttClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(MqttClientFactory.class);
    private static final ScheduledExecutorService CONNECT_EXECUTOR = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "MQTT-Connect-Task");
        t.setDaemon(true);
        return t;
    });
    private static final ExecutorService SUBSCRIBE_EXECUTOR = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "MQTT-Subscribe-Task");
        t.setDaemon(true);
        return t;
    });
    private static Map<String, MqttConnection> CLIENT_MAP = Maps.newConcurrentMap();
    private static ReentrantLock REENTRANT_LOCK = new ReentrantLock();

    static {
        CONNECT_EXECUTOR.scheduleWithFixedDelay(() -> heartbeat(), 0, 30, TimeUnit.SECONDS);
    }

    /**
     * 连接心跳
     */
    private static void heartbeat() {
        try {
            for (Map.Entry<String, MqttConnection> entry : CLIENT_MAP.entrySet()) {
                connect(entry.getValue());
            }
        } catch (Exception ex) {
            LOGGER.error("MQTT-Heartbeat error,ex:{}", ExceptionUtils.getStackTrace(ex));
        }
    }

    /**
     * 连接
     *
     * @param connection
     */
    public static void connect(MqttConnection connection) {
        try {
            MqttClient mqttClient = connection.getMqttClient();
            MqttConnectOptions options = connection.getOptions();
            if (null == mqttClient) {
                LOGGER.info("MQTT Connecting error,mqttClient is null,broker:{}", connection.getUrl());
                return;
            }
            if (null == options) {
                LOGGER.info("MQTT Connecting error,options is null,broker:{}", connection.getUrl());
                return;
            }
            if (mqttClient.isConnected()) {
                return;
            }
            LOGGER.info("MQTT Connecting,broker:{}", connection.getUrl());
            mqttClient.connect(options);
            LOGGER.info("MQTT Connected,broker:{}", connection.getUrl());
        } catch (MqttException ex) {
            LOGGER.error("MQTT Connecting error,clientKey:{},code:{},msg:{}", connection.getClientKey(), ex.getReasonCode(), ex.getMessage());
        } catch (Exception ex) {
            LOGGER.error("MQTT Connecting error,clientKey:{},ex:{}", connection.getClientKey(), ExceptionUtils.getStackTrace(ex));
        }
    }

    public synchronized static void removeClient(String clientKey) {
        MqttConnection hasConnection = CLIENT_MAP.get(clientKey);
        if (hasConnection != null) {
            int count = hasConnection.getReferenceCount() - 1;
            if (count <= 0) {
                CLIENT_MAP.remove(clientKey);
                MqttClient client = hasConnection.getMqttClient();
                try {
                    if (client != null && client.isConnected()) {
                        client.disconnect();
                        client.close();
                    }
                } catch (MqttException e) {
                    throw new RuntimeException("PahoSink close error", e);
                }
            } else {
                hasConnection.setReferenceCount(count);
            }
        }
    }

    public synchronized static MqttClient getClient(MqttConnection connection) {
        String clientKey = connection.getClientKey();
        MqttConnection connected = CLIENT_MAP.get(clientKey);
        if (connected != null) {
            connected.setReferenceCount(connected.getReferenceCount() + 1);
            MqttClient mqttClient = connected.getMqttClient();
            return mqttClient;
        }
        try {
            boolean success = REENTRANT_LOCK.tryLock(5000, TimeUnit.SECONDS);
            if (!success) {
                LOGGER.info("MQTT getClient timeout,clientId:{},broker:{}", connection.getClientId(), connection.getUrl());
                return null;
            } else {
                //已经存在
                connected = CLIENT_MAP.get(clientKey);
                if (connected != null) {
                    connected.setReferenceCount(connected.getReferenceCount() + 1);
                    MqttClient mqttClient = connected.getMqttClient();
                    return mqttClient;
                }
            }
            String uniqueId = connection.getClientId() + "_" + RandomStringUtils.randomAlphanumeric(6);
            MqttClient mqttClient = new MqttClient(connection.getUrl(), uniqueId, new MemoryPersistence());
            //最多等待1000ms
            mqttClient.setTimeToWait(1000);
            //设置回调
            mqttClient.setCallback(new MqttClientCallback(connection));
            connection.setMqttClient(mqttClient);
            //触发连接
            if (connection.isAutoConnect()) {
                connect(connection);
            }
            CLIENT_MAP.put(connection.getClientKey(), connection);
            return mqttClient;
        } catch (MqttException ex) {
            LOGGER.info("MQTT getClient error,clientId:{},broker:{},code:{},msg:{}", connection.getClientId(), connection.getUrl(), ex.getReasonCode(), ex.getMessage());
        } catch (Exception ex) {
            LOGGER.info("MQTT getClient error,clientId:{},broker:{},ex:{}", connection.getClientId(), connection.getUrl(), ExceptionUtils.getStackTrace(ex));
        } finally {
            REENTRANT_LOCK.unlock();
        }
        return null;
    }

    /**
     * MQTT 回调
     */
    public static class MqttClientCallback implements MqttCallbackExtended {
        private MqttConnection clientInfo;

        public MqttClientCallback(MqttConnection clientInfo) {
            this.clientInfo = clientInfo;
        }

        /**
         * 客户端连接成功后就需要尽快订阅需要的Topic。
         */
        @Override
        public void connectComplete(boolean reconnect, String serverURI) {
            String topic = clientInfo.getTopic();
            LOGGER.info("MQTT Subscribe topic:{},broker:{}", topic, serverURI);
            SUBSCRIBE_EXECUTOR.submit(() -> {
                try {
                    clientInfo.getMqttClient().subscribe(topic);
                } catch (MqttException ex) {
                    LOGGER.error("MQTT Subscribe error,clientKey:{},topic:{},code:{},message:{}", clientInfo.getClientKey(), topic, ex.getReasonCode(), ex.getMessage());
                }
            });
        }

        /**
         * 连接丢失
         *
         * @param throwable
         */
        @Override
        public void connectionLost(Throwable throwable) {
            connect(clientInfo);
        }

        @Override
        public void messageArrived(String topic, MqttMessage message) {
            MessageProcess messageProcess = clientInfo.getMessageProcess();
            if (messageProcess == null) {
                return;
            }
            try {
                messageProcess.process(topic, message);
            } catch (Exception ex) {
                LOGGER.error("MQTT process message error,topic:{},ex:{}", topic, ExceptionUtils.getStackTrace(ex));
            }
        }

        @Override
        public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

        }

    }

}
