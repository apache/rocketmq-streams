## rocketmq-streams-examples

### 1、fileSource example

```java
public class FileSourceExample {
    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        source.fromFile("data.txt", false)
                .map(message -> message)
                .toPrint(1)
                .start();
    }
}

```


### 2、rocketmq window example

#### 2.1 安装rocketmq
可以参考[rocketmq搭建文档见文档](https://rocketmq.apache.org/docs/quick-start/)

#### 2.2 代码示例
```java
//1、produce message to rocketmq
public class ProducerFromFile {

    public static void produce(String filePath) {
        try {
            DefaultMQProducer producer = new DefaultMQProducer("test-group");
            producer.setNamesrvAddr(NAMESRV_ADDRESS);
            producer.start();

            List<String> result = ProducerFromFile.read(filePath);

            for (String str : result) {
                Message msg = new Message(RMQ_TOPIC, "", str.getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
            }
            //Shut down once the producer instance is not longer in use.
            producer.shutdown();
        } catch (Throwable t) {
            t.printStackTrace();
        }

    }

    private static File getFile(String filePath) {
        File file = new File(filePath);
        if (!file.exists()) {
            ClassLoader loader = ProducerFromFile.class.getClassLoader();
            URL url = loader.getResource(filePath);

            if (url != null) {
                String path = url.getFile();
                file = new File(path);
            }
        }
        return file;

    }

    private static List<String> read(String path) {
        File file = getFile(path);
        List<String> result = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(file));

            String line = reader.readLine();
            while (line != null) {
                result.add(line);
                line = reader.readLine();
            }

        } catch (Throwable t) {
            t.printStackTrace();
        }
        return result;
    }
}


// 消费rocketmq中数据，并计算
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
        ProducerFromFile.produce("data.txt");

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
                .window(TumblingWindow.of(Time.seconds(5)))
                .groupBy("ProjectName", "LogStore")
                .sum("OutFlow", "OutFlow")
                .sum("InFlow", "InFlow")
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
可以在org.apache.rocketmq.streams.examples.rocketmqsource.RocketmqWindowTest中直接运行这个例子

#### 2.3 结果说明
这个例子中，使用rocketmq-streams消费rocketmq中的数据，并按照ProjectName和LogStore两个字段联合分组统计，两个字段的值相同，分为一组。统计每组的InFlow和OutFlow。
data.text数据运行的结果部分如下：

"InFlow":22,"total":4,"ProjectName":"ProjectName-0","LogStore":"LogStore-0","OutFlow":18
"InFlow":18,"total":3,"ProjectName":"ProjectName-2","LogStore":"LogStore-2","OutFlow":15
"InFlow":15,"total":3,"ProjectName":"ProjectName-1","LogStore":"LogStore-1","OutFlow":12


