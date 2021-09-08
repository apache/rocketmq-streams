## rocketmq-streams-examples

### 1、fileSource example
```java
public class FileSourceExample {
    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        source.fromFile("/your/file/path", false)
                .map(message -> message)
                .toPrint(1)
                .start();
    }
}

```


### 2、rocketmq window example
```java
public class RocketmqWindowTest {
    public static final String NAMESRV_ADDRESS = "127.0.0.1:9876";
    public static final String RMQ_TOPIC = "NormalTestTopic";
    public static final String RMQ_CONSUMER_GROUP_NAME = "group-01";
    public static final String TAGS = "*";

    /**
     * 1、before run this case, make sure some data has already been rocketmq.
     * 2、rocketmq allow create topic automatically.
     */
    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");

        source.fromRocketmq(
                RMQ_TOPIC,
                RMQ_CONSUMER_GROUP_NAME,
                false,
                NAMESRV_ADDRESS)
                .filter((message) -> {
                    try {
                        JSONObject.parseObject((String) message);
                    } catch (Throwable t) {
                        // if can not convert to json, discard it.because all operator are base on json.
                        return true;
                    }
                    return false;
                })
                //must convert message to json.
                .map(message -> JSONObject.parseObject((String) message))
                .window(TumblingWindow.of(Time.seconds(1)))
                .groupBy("field-1", "field-2")
                .count("total")
                .waterMark(1)
                .setLocalStorageOnly(true)
                .toDataSteam()
                .toPrint(1)
                .with(WindowStrategy.highPerformance())
                .start();

    }

}

```




