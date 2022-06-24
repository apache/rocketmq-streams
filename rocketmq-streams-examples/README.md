## rocketmq-streams-examples


### 1、File source example

#### 1.1 从`scores.txt`逐行读取文件数据

根据分数`score`字段筛选，输出每人大于90分的科目

```java
public class FileSourceExample {
    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }
        System.out.println("begin streams code.");

        source.fromFile("scores.txt", true)
                .map(message -> message)
                .filter(message -> ((JSONObject) message).getInteger("score") > 90)
                .selectFields("name", "subject")
                .toFile("./rocketmq-streams-examples/src/main/resources/result.txt")
                .start();

    }
}

```


#### 1.2 代码示例

[代码示例 ProducerFromFile.java](./../rocketmq-streams-examples/src/main/java/org/apache/rocketmq/streams/examples/source/FileSourceExample.java)

#### 1.3 源数据

[源数据 scores.txt](./../rocketmq-streams-examples/src/main/resources/scores.txt)

```text
{"name":"张三","class":"3","subject":"数学","score":90}
{"name":"张三","class":"3","subject":"历史","score":81}
{"name":"张三","class":"3","subject":"英语","score":91}
{"name":"张三","class":"3","subject":"语文","score":70}
{"name":"张三","class":"3","subject":"政治","score":84}
{"name":"张三","class":"3","subject":"地理","score":99}
{"name":"李四","class":"3","subject":"数学","score":76}
{"name":"李四","class":"3","subject":"历史","score":83}
{"name":"李四","class":"3","subject":"英语","score":82}
{"name":"李四","class":"3","subject":"语文","score":92}
{"name":"李四","class":"3","subject":"政治","score":97}
{"name":"李四","class":"3","subject":"地理","score":89}
{"name":"王五","class":"3","subject":"数学","score":86}
{"name":"王五","class":"3","subject":"历史","score":88}
{"name":"王五","class":"3","subject":"英语","score":86}
{"name":"王五","class":"3","subject":"语文","score":93}
{"name":"王五","class":"3","subject":"政治","score":99}
{"name":"王五","class":"3","subject":"地理","score":88}

```


### 2、分时间段，统计分组中某字段的和


#### 2.1 安装 Apache RocketMQ
可以参考[Apache RocketMQ 搭建文档](https://rocketmq.apache.org/docs/quick-start/)

#### 2.2 源数据
[源数据](./../rocketmq-streams-examples/src/main/resources/data.txt)
```xml
{"InFlow":"1","ProjectName":"ProjectName-0","LogStore":"LogStore-0","OutFlow":"0"}
{"InFlow":"2","ProjectName":"ProjectName-1","LogStore":"LogStore-1","OutFlow":"1"}
{"InFlow":"3","ProjectName":"ProjectName-2","LogStore":"LogStore-2","OutFlow":"2"}
{"InFlow":"4","ProjectName":"ProjectName-0","LogStore":"LogStore-0","OutFlow":"3"}
{"InFlow":"5","ProjectName":"ProjectName-1","LogStore":"LogStore-1","OutFlow":"4"}
{"InFlow":"6","ProjectName":"ProjectName-2","LogStore":"LogStore-2","OutFlow":"5"}
{"InFlow":"7","ProjectName":"ProjectName-0","LogStore":"LogStore-0","OutFlow":"6"}
{"InFlow":"8","ProjectName":"ProjectName-1","LogStore":"LogStore-1","OutFlow":"7"}
{"InFlow":"9","ProjectName":"ProjectName-2","LogStore":"LogStore-2","OutFlow":"8"}
{"InFlow":"10","ProjectName":"ProjectName-0","LogStore":"LogStore-0","OutFlow":"9"}
```

#### 2.3 代码示例

[代码示例 RocketMQWindowExample.java](./../rocketmq-streams-examples/src/main/java/org/apache/rocketmq/streams/examples/aggregate/RocketMQWindowExample.java)


#### 2.4 结果说明
这个例子中，使用 rocketmq-streams 消费 rocketmq 中的数据，并按照 ProjectName 和 LogStore 两个字段联合分组统计，两个字段的值相同，分为一组。
分别统计每组的InFlow和OutFlow两字段累计和。

data.text数据运行的结果部分如下：

```xml
"InFlow":22,"total":4,"ProjectName":"ProjectName-0","LogStore":"LogStore-0","OutFlow":18
"InFlow":18,"total":3,"ProjectName":"ProjectName-2","LogStore":"LogStore-2","OutFlow":15
"InFlow":15,"total":3,"ProjectName":"ProjectName-1","LogStore":"LogStore-1","OutFlow":12
```
可见"ProjectName":"ProjectName-0","LogStore":"LogStore-0"分组公有4条数据，"ProjectName":"ProjectName-2","LogStore":"LogStore-2"，3条数据。
"ProjectName":"ProjectName-1","LogStore":"LogStore-1"分组3条数据，总共10条数据。结果与源数据一致。

### 3、网页点击统计
#### 3.1、数据说明
原始数据为resources路径下的[pageClickData.txt](./../rocketmq-streams-examples/src/main/resources/pageClickData.txt)

第一列是用户id，第二列是用户点击时间，最后一列是网页地址
```xml
{"userId":"1","eventTime":"1631700000000","method":"GET","url":"page-1"}
{"userId":"2","eventTime":"1631700030000","method":"POST","url":"page-2"}
{"userId":"3","eventTime":"1631700040000","method":"GET","url":"page-3"}
{"userId":"1","eventTime":"1631700050000","method":"DELETE","url":"page-2"}
{"userId":"1","eventTime":"1631700060000","method":"DELETE","url":"page-2"}
{"userId":"2","eventTime":"1631700070000","method":"POST","url":"page-3"}
{"userId":"3","eventTime":"1631700080000","method":"GET","url":"page-1"}
{"userId":"1","eventTime":"1631700090000","method":"GET","url":"page-2"}
{"userId":"2","eventTime":"1631700100000","method":"PUT","url":"page-3"}
{"userId":"4","eventTime":"1631700120000","method":"POST","url":"page-1"}
```

#### 3.2、统计某段时间窗口内用户点击网页次数
[代码示例 UsersDimensionExample.java](./../rocketmq-streams-examples/src/main/java/org/apache/rocketmq/streams/examples/aggregate/UsersDimensionExample.java)

结果：
```xml
{"start_time":"2021-09-15 18:00:00","total":1,"windowInstanceId":"SPVGTV6DaXmxV5mGNzQixQ==","offset":53892061100000001,"end_time":"2021-09-15 18:01:00","userId":"2"}
{"start_time":"2021-09-15 18:00:00","total":1,"windowInstanceId":"dzAZ104qjUAwzTE6gbKSPA==","offset":53892061100000001,"end_time":"2021-09-15 18:01:00","userId":"3"}
{"start_time":"2021-09-15 18:00:00","total":2,"windowInstanceId":"wrTTyU5DiDkrAb6669Ig9w==","offset":53892061100000001,"end_time":"2021-09-15 18:01:00","userId":"1"}
{"start_time":"2021-09-15 18:01:00","total":1,"windowInstanceId":"vabkmx14xHsJ7G7w16vwug==","offset":53892121100000001,"end_time":"2021-09-15 18:02:00","userId":"3"}
{"start_time":"2021-09-15 18:01:00","total":2,"windowInstanceId":"YIgEKptN2Wf+Oq2m8sEcYw==","offset":53892121100000001,"end_time":"2021-09-15 18:02:00","userId":"2"}
{"start_time":"2021-09-15 18:01:00","total":2,"windowInstanceId":"iYKnwMYAzXFJYbO1KvDnng==","offset":53892121100000001,"end_time":"2021-09-15 18:02:00","userId":"1"}
{"start_time":"2021-09-15 18:02:00","total":1,"windowInstanceId":"HBojuU6/2F/6llkyefECxw==","offset":53892181100000001,"end_time":"2021-09-15 18:03:00","userId":"4"}
```

在时间范围 18:00:00- 18:01:00内：

|userId|点击次数|
|------|---|
|   1  | 2 |
|   2  | 1 |
|   3  | 1 |

在时间范围 18:01:00- 18:02:00内：

|userId|点击次数|
|------|---|
|   1  | 2 |
|   2  | 2 |
|   3  | 1 |

在时间范围 18:02:00- 18:03:00内：

|userId|点击次数|
|------|---|
|   4  | 1 | 

可查看原数据文件，eventTime为时间字段，简单检查后上述结果与预期相符合。

#### 3.3、统计某段时间窗口内，被点击次数最多的网页
[代码示例 PageDimensionExample.java](./../rocketmq-streams-examples/src/main/java/org/apache/rocketmq/streams/examples/aggregate/PageDimensionExample.java)

运行结果：
```xml
{"start_time":"2021-09-15 18:00:00","total":1,"windowInstanceId":"wrTTyU5DiDkrAb6669Ig9w==","offset":53892061100000001,"end_time":"2021-09-15 18:01:00","url":"page-1"}
{"start_time":"2021-09-15 18:00:00","total":2,"windowInstanceId":"seECZRcaQSRsET1rDc6ZAw==","offset":53892061100000001,"end_time":"2021-09-15 18:01:00","url":"page-2"}
{"start_time":"2021-09-15 18:00:00","total":1,"windowInstanceId":"dzAZ104qjUAwzTE6gbKSPA==","offset":53892061100000001,"end_time":"2021-09-15 18:01:00","url":"page-3"}
{"start_time":"2021-09-15 18:01:00","total":2,"windowInstanceId":"uCqvAeaLTYRnjQm8dCZOvw==","offset":53892121100000001,"end_time":"2021-09-15 18:02:00","url":"page-2"}
{"start_time":"2021-09-15 18:01:00","total":3,"windowInstanceId":"vabkmx14xHsJ7G7w16vwug==","offset":53892121100000001,"end_time":"2021-09-15 18:02:00","url":"page-3"}
{"start_time":"2021-09-15 18:02:00","total":1,"windowInstanceId":"NdgwYMT8azNMu55NUIvygg==","offset":53892181100000001,"end_time":"2021-09-15 18:03:00","url":"page-1"}

```
在时间窗口18:00:00 - 18:01:00 内，有4条数据；

在时间窗口18:01:00 - 18:02:00 内，有5条数据；

在时间窗口18:02:00 - 18:03:00 内，有1条数据；

分钟统计窗口内，被点击次数最多的网页.
得到上述数据后，需要按照窗口进行筛选最大值，需要再次计算。
代码：
```java
    public void findMax() {
        DataStreamSource source = StreamBuilder.dataStream("ns-1", "pl-1");
        source.fromFile("/home/result.txt", false)
        .map(message -> JSONObject.parseObject((String) message))
        .window(TumblingWindow.of(Time.seconds(5)))
        .groupBy("start_time","end_time")
        .max("total")
        .waterMark(1)
        .setLocalStorageOnly(true)
        .toDataSteam()
        .toPrint(1)
        .start();
   }

```
得到结果：
```xml
{"start_time":"2021-09-17 11:09:35","total":"2","windowInstanceId":"kRRpe2hPEQtEuTkfnXUaHg==","offset":54040181100000001,"end_time":"2021-09-17 11:09:40"}
{"start_time":"2021-09-17 11:09:35","total":"3","windowInstanceId":"kRRpe2hPEQtEuTkfnXUaHg==","offset":54040181100000002,"end_time":"2021-09-17 11:09:40"}
{"start_time":"2021-09-17 11:09:35","total":"1","windowInstanceId":"kRRpe2hPEQtEuTkfnXUaHg==","offset":54040181100000003,"end_time":"2021-09-17 11:09:40"}
```

可以得到三个窗口中网页点击次数最多分别是2次，1次，3次。

### 4、Rocketmq-streams 多客户端消费
#### 4.1、数据说明
源数据由[data.txt](./../rocketmq-streams-examples/src/main/resources/data.txt)组成，反复发送100遍，总共生产1000条数据。
#### 4.2、代码实例
[代码示例 MultiStreamsExample.java](./../rocketmq-streams-examples/src/main/java/org/apache/rocketmq/streams/examples/mutilconsumer/MultiStreamsExample.java)

代码中读取data.txt文件反复发送100遍，发送1000条数据。同时，开启两个消费者，每个消费者独立消费数据，然后做window聚合。
代码目的：
    通过两个独立消费者，组成消费者组，同时消费相同topic数据，达到当1个消费者消费不过来时横向扩容的效果，通过打印出来"total"字段值的和判断两个消费者是否总共消费了1000条数据。

#### 4.3、结果说明
结果数据下所示，可计算各行total对应值之和为1000，表明的却两个消费者达到了并发消费的效果，计算无误，达到了扩容目的。
```xml

{"start_time":"2021-09-27 14:10:10","InFlow":1144,"total":208,"windowInstanceId":"gYZ3tv/5ohgHrwF6tIFgoQ==","offset":54915025100000001,"ProjectName":"ProjectName-0","LogStore":"LogStore-0","end_time":"2021-09-27 14:10:20","OutFlow":936}
{"start_time":"2021-09-27 14:10:10","InFlow":936,"total":156,"windowInstanceId":"gYZ3tv/5ohgHrwF6tIFgoQ==","offset":54915025100000002,"ProjectName":"ProjectName-2","LogStore":"LogStore-2","end_time":"2021-09-27 14:10:20","OutFlow":780}
{"start_time":"2021-09-27 14:10:10","InFlow":780,"total":156,"windowInstanceId":"gYZ3tv/5ohgHrwF6tIFgoQ==","offset":54915025100000003,"ProjectName":"ProjectName-1","LogStore":"LogStore-1","end_time":"2021-09-27 14:10:20","OutFlow":624}
{"start_time":"2021-09-27 14:10:20","InFlow":1056,"total":192,"windowInstanceId":"4YnbFAgSzeDt5qpo+Is/5w==","offset":54915035100000001,"ProjectName":"ProjectName-0","LogStore":"LogStore-0","end_time":"2021-09-27 14:10:30","OutFlow":864}
{"start_time":"2021-09-27 14:10:20","InFlow":720,"total":144,"windowInstanceId":"4YnbFAgSzeDt5qpo+Is/5w==","offset":54915035100000002,"ProjectName":"ProjectName-1","LogStore":"LogStore-1","end_time":"2021-09-27 14:10:30","OutFlow":576}
{"start_time":"2021-09-27 14:10:20","InFlow":864,"total":144,"windowInstanceId":"4YnbFAgSzeDt5qpo+Is/5w==","offset":54915035100000003,"ProjectName":"ProjectName-2","LogStore":"LogStore-2","end_time":"2021-09-27 14:10:30","OutFlow":720}

```
