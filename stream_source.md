# 创建实时任务数据源

通过`` DataStreamSource dataStreamSource = StreamBuilder.dataStream("test_namespace", "graph_pipeline");``完成dataStreamSource的构建后，就可以通过dataStreamSource来创建数据源了

我们可以从如下数据中创建数据源

## 数组

```java
    Object[] array = .....; //数组
    DataStream dataStream = dataStreamSource.fromArray(array);
```

## 文件

```java
    String filepath = .....; //文件路径
    DataStream dataStream = dataStreamSource.fromFile(filePath);
```
或者

```java
    String filepath = .....;  //文件路径
    Boolean isJsonData = true;   //是否json格式
    DataStream dataStream = dataStreamSource.fromFile(filePath, isJsonData);
```


## Rocketmq

```java

     String topic = .....; //rocketmq 的topic
     String groupName = .....; // rocketmq的消费组
     String namesrvAddress = ......; //rocketmq的nameserver
     DataStream dataStream = dataStreamSource.fromRocketmq(topic, groupName, namesrvAddress);

```
或者

```java

    String topic = .....; //rocketmq 的topic
    String groupName = .....; // rocketmq的消费组
    String namesrvAddress = ......; //rocketmq的nameserver
    Boolean isJsonData = true; //是否json     
    DataStream dataStream = dataStreamSource.fromRocketmq(topic, groupName, isJsonData, namesrvAddress);

```
或者
```java

    String topic = .....; //rocketmq 的topic
    String groupName = .....; // rocketmq的消费组
    String namesrvAddress = ......; //rocketmq的nameserver
    Boolean isJsonData = true; //是否json     
    String tags = ......; // rocketmq的tag信息
    DataStream dataStream = dataStreamSource.fromRocketmq(topic, groupName, tags, isJsonData, namesrvAddress);

```

## kafka
```java
    String bootstrapServers = ......;//kafka的bootstrap server
    String topic = ......; //kafka的topic
    String groupName = ......; //kafka的消费组
    Boolean isJsonData = true; //是否json
    Integer maxThread = 1; //客户端线程数
    DataStream dataStream = dataStreamSource.fromKafka(bootstrapServers, topic, groupName, isJsonData, maxThread);
```
或者
```java
    String bootstrapServers = ......;//kafka的bootstrap server
    String topic = ......; //kafka的topic
    String groupName = ......; //kafka的消费组
    Boolean isJsonData = true; //是否json
    DataStream dataStream = dataStreamSource.fromKafka(bootstrapServers, topic, groupName, isJsonData);
```
或者
```java
    String bootstrapServers = ......;//kafka的bootstrap server
    String topic = ......; //kafka的topic
    String groupName = ......; //kafka的消费组
    DataStream dataStream = dataStreamSource.fromKafka(bootstrapServers, topic, groupName);
```




# MQTT协议
```java

    String url = ......;
    String clientId = ......;
    String topic = ......;
    DataStream dataStream = dataStreamSource.fromMqtt(url, cliientId, topic);

```
或者
```java

    String url = ......;
    String clientId = ......;
    String topic = ......;
    String username = ......;
    String password = ......;
    DataStream dataStream = dataStreamSource.fromMqtt(url, cliientId, topic, username, password);

```
或者
````java

    String url = ......;
    String clientId = ......;
    String topic = ......;
    String username = ......;
    String password = ......;
    Boolean cleanSession = true;
    Integer connectionTimeout = 10;
    Integer aliveInterval = 60;
    Boolean automatiicReconnect = true;
    DataStream dataStream = dataStreamSource.fromMqtt(url, cliientId, topic, username, password, cleanSession, connectionTimeout, aliveInterval, automaticReconnect);

````

##自定义Source
````java
    DataStream dataStream = dataStreamSource.from(new ISource<ISource>() {});
````
