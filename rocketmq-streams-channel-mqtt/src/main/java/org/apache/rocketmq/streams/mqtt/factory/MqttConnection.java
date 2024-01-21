package org.apache.rocketmq.streams.mqtt.factory;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;

/**
 * @author fgm
 * @date 2023/5/7
 * @description Mqtt连接信息
 */
public class MqttConnection {

    protected int referenceCount = 0;
    //输入属性
    private String url;
    private String topic;
    private String username;
    private String password;
    private String clientId;
    private MqttConnectOptions options;
    //是否自动连接
    private boolean autoConnect;
    //消息处理
    private MessageProcess messageProcess;
    //生成属性
    private String clientKey;
    private MqttClient mqttClient;

    public MqttConnection(String url, String topic, String username, String password, String clientId, MqttConnectOptions options) {
        this.url = url;
        this.topic = topic;
        this.username = username;
        this.password = password;
        this.clientId = clientId;
        this.options = options;
        this.clientKey = this.url + "#" + this.topic + "#" + this.username + "#" + this.password + "#" + this.clientId;
    }

    public String getClientKey() {
        return clientKey;
    }

    public int getReferenceCount() {
        return referenceCount;
    }

    public void setReferenceCount(int referenceCount) {
        this.referenceCount = referenceCount;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
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

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public MqttConnectOptions getOptions() {
        return options;
    }

    public void setOptions(MqttConnectOptions options) {
        this.options = options;
    }

    public MqttClient getMqttClient() {
        return mqttClient;
    }

    public void setMqttClient(MqttClient mqttClient) {
        this.mqttClient = mqttClient;
    }

    public boolean isAutoConnect() {
        return autoConnect;
    }

    public void setAutoConnect(boolean autoConnect) {
        this.autoConnect = autoConnect;
    }

    public MessageProcess getMessageProcess() {
        return messageProcess;
    }

    public void setMessageProcess(MessageProcess messageProcess) {
        this.messageProcess = messageProcess;
    }
}
