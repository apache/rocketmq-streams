package org.apache.rocketmq.streams.mqtt.factory;

import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * @author fgm
 * @date 2023/5/16
 * @description MQTT消息回调
 */
public interface MessageProcess {

    /**
     * 处理消息
     *
     * @param topic
     * @param message
     */
    void process(String topic, MqttMessage message);
}
