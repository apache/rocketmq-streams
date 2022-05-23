# Summary


## [中文文档](./README-chinese.md)

## [Quick Start](./quick_start.md)

## Features

* Lightweight deployment: RocketMQ Streams can be deployed separately or in cluster mode.
* Various types of data input and output: source supports [RocketMQ](https://github.com/apache/rocketmq) while sink supports databases and RocketMQ, etc.

## DataStream Example

```java
import org.apache.rocketmq.streams.client.transform.DataStream;

DataStreamSource source=StreamBuilder.dataStream("namespace","pipeline");
    source
    .fromFile("～/admin/data/text.txt",false)
    .map(message->message)
    .toPrint(1)
    .start();
```

## Maven Repository

```xml

<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-streams-clients</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

# Core API

RocketMQ Streams implements a series of advanced APIs, allowing users to write stream computing programs conveniently and achieve their own business requirements.

## StreamBuilder

StreamBuilder is used to build the source of stream tasks. It contains two methods: ```dataStream()``` and ```tableStream()```, which return two sources, DataStreamSource and TableStreamSource, respectively.

+ [dataStream(nameSpaceName,pipelineName)]() returns an instance of DataStreamSource, used for segmented programming to achieve stream computing tasks.
+ [tableStream(nameSpaceName,pipelineName)]() returns an instance of TableStreamSource, used for script programming to achieve stream computing tasks.

## DataStream API

### Source

DataStreamSource is a source class of segmented programming, used to interface with various data sources and obtain data from major message queues.

+ ```fromFile```: reads data from the file. This method contains two parameters:
    + ```filePath```: specifies which file path to read. Required.
    + ```isJsonData```: specifies whether data is in JSON format. Optional. Default value: ```true```.
    + ```tags```: the tags for filtering messages used by the RocketMQ consumer. Optional.


+ ```fromRocketmq```: obtains data from RocketMQ, including four parameters:
    + ```topic```:  the topic name of RocketMQ. Required.
    + ```groupName```: the name of the consumer group. Required.
    + ```isJson```: specifies whether data is in JSON format. Optional.
    + ```tags```: the tags for filtering messages used by the RocketMQ consumer. Optional.

+ ```from```: custom data source. You can specify your own data source by implementing ISource interface.

### transform

transform allows the input source data to be modified during the stream calculation process for the next step; DataStream API includes ```DataStream```, ```JoinStream```, ```SplitStream```, ```WindowStream```, and many other transform classes.

#### DataStream

DataStream implements a series of common stream calculation operators as follows:

+ ```map```: returns a new DataStream by passing each record of the source to the **func** function.
+ ```flatmap```: similar to map. One input item corresponds to 0 or more output items.
+ ```filter```: returns a new DataStream based on the record of the source DataStream only when the ** func** function returns **true**.
+ ```forEach```: executes the **func** function once for each record and returns a new DataStream.
+ ```selectFields```: returns the corresponding field value for each record, and returns a new DataStream.
+ ```operate```: executes a custom function for each record and returns a new DataStream.
+ ```script```: executes a script for each recorded field, returns new fields, and generates a new DataStream.
+ ```toPrint```: prints the result on the console and generates a new DataStreamAction instance.
+ ```toFile```: saves the result as a file and generates a new DataStreamAction instance.
+ ```toDB```: saves the result to the database.
+ ```toRocketmq```: outputs the result to RocketMQ.
+ ```to```: outputs the result to the specified storage through the custom ISink interface.
+ ```window```: performs relevant statistical analysis in the window, generally used in conjunction with ```groupBy```. ```window()``` is used to define the size of the window, and ```groupBy( )``` used to define the main key of statistical analysis. You can specify multiple main keys:
    + ```count```: counts in the window.
    + ```min```: gets the minimum of the statistical value in the window.
    + ```max```: gets the maximum of the statistical value in the window.
    + ```avg```: gets the average of the statistical values in the window.
    + ```sum```: gets the sum of the statistical values in the window.
    + ```reduce```: performs custom summary calculations in the window.
+ ```join```: associates the two streams or one stream and one physical table according to the conditions and merges them into a large stream for related calculations.
    + ```dimJoin```  associate a stream with a physical table which can be a file or a db table, and all matching records are retained
    + ```dimLeftJoin```  After a flow is associated with a physical table, all data of the flow is reserved and fields that do not match the physical table are left blank
    + ```join```
    + ```leftJoin```
+ ```union```: merges the two streams.
+ ```split```: splits a data stream into different data streams according to tags for downstream analysis and calculation.
+ ```with```: specifies related strategies during the calculation, including Checkpoint and state storage strategies, etc.

# Strategy

The Strategy mechanism is mainly used to control the underlying logic during the operation of the computing engine, such as the storage methods of Checkpoint and state etc. Subsequent controls for windows, dual-stream joins, and so on will be added. All control strategies are transmitted through the ```with``` operator. Multiple policy types can be transmitted at the same time.

```java
//Specify the storage strategy for Checkpoint.
source
    .fromRocketmq("TSG_META_INFO","")
    .map(message->message+"--")
    .toPrint(1)
    .with(CheckpointStrategy.db("jdbc:mysql://XXXXX:3306/XXXXX","","",0L))
    .start();
```

=======
* [Quick Start](quick\_start.md)
* [创建实时任务数据源](stream\_source.md)
* [创建实时任务数据输出](stream\_sink.md)
* [数据处理逻辑](stream\_transform.md)
>>>>>>> 1cd2dd0291dbcab033e6773021ddca13ce819f82
