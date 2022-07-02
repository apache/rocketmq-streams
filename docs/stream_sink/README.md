# 创建实时任务数据输出

我们可以将实时任务处理的结果以如下的方式输出

## 打印

```java
    dataStream.toPrint();
```

或者

```java
    dataStream.toPrint(batchSize);
```

## 文件

```java
    String filePath=......;
    Integer batchSize=.....;
    Boolean isAppend=true;
    dataStream.toFile(filePath,batchSize,isAppend);
```

或者

```java
    String filePath=......;
    Boolean isAppend=true;
    dataStream.toFile(filePath,isAppend);
```

或者

```java
    String filePath=......;
    dataStream.toFile(filePath);
```

## DB

```java
    String url=......;
    String userName=.....;
    String password=......;
    String tableName=......;
    dataStream.toDB(url,userName,password,tableName);
```

## Rocketmq

```java

    String topic=.....; //rocketmq 的topic
    String namesrvAddress=......; //rocketmq的nameserver
    DataStream dataStream=dataStreamSource.toRocketmq(topic,namesrvAddress);

```

或者

```java

    String topic=.....; //rocketmq 的topic
    String groupName=.....; // rocketmq的消费组
    String namesrvAddress=......; //rocketmq的nameserver
    DataStream dataStream=dataStreamSource.toRocketmq(topic,groupName,namesrvAddress);

```

或者

```java

    String topic=.....; //rocketmq 的topic
    String groupName=.....; // rocketmq的消费组
    String namesrvAddress=......; //rocketmq的nameserver
    String tags=......; // rocketmq的tag信息
    DataStream dataStream=dataStreamSource.toRocketmq(topic,tags,groupName,namesrvAddress);

```

# MQTT协议

```java

    String url=......;
    String clientId=......;
    String topic=......;
    DataStream dataStream=dataStreamSource.toMqtt(url,cliientId,topic);

```

或者

```java

    String url=......;
    String clientId=......;
    String topic=......;
    String username=......;
    String password=......;
    DataStream dataStream=dataStreamSource.toMqtt(url,cliientId,topic,username,password);

```

## 自定义Source

````java
    dataStreamSource.to(new ISink<ISource>(){});
````
