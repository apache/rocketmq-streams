# RocketMQ Streams
[![Build Status](https://travis-ci.org/apache/rocketmq-streams.svg?branch=main)](https://travis-ci.com/github/apache/rocketmq-streams)
[![CodeCov](https://codecov.io/gh/apache/rocketmq-stream/branch/main/graph/badge.svg)](https://codecov.io/gh/apache/rocketmq-steams)
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://github.com/apache/rocketmq-streams/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)
## Features

* 轻量级部署：可以单独部署，也支持集群部署
* 多种类型的数据输入以及输出，source 支持 rocketmq ， sink 支持db, rocketmq 等

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

rocketmq-stream 实现了一系列高级的API，可以让用户很方便的编写流计算的程序，实现自己的业务需求；

## StreamBuilder

StreamBuilder 用于构建流任务的源； 内部包含```dataStream()```和```tableStream()```俩个方法，分别返回DataStreamSource和TableStreamSource俩个源；

+ [dataStream(nameSpaceName,pipelineName)]() 返回DataStreamSource实例，用于分段编程实现流计算任务；
+ [tableStream(nameSpaceName,pipelineName)]()返回TableStreamSource实例， 用于脚本编程实现流计算任务；

## DataStream API

### Source

DataStreamSource 是分段式编程的源头类，用于对接各种数据源， 从各大消息队列中获取数据；

+ ```fromFile```  从文件中读取数据， 该方法包含俩个参数
    + ```filePath``` 文件路径，必填参数
    + ```isJsonData```  是否json数据， 非必填参数， 默认为```true```


+ ```fromRocketmq``` 从rocketmq中获取数据，包含四个参数
    + ```topic``` rocketmq消息队列的topic名称，必填参数
    + ```groupName``` 消费者组的名称，必填参数
    + ```isJson``` 是否json格式，非必填参数
    + ```tags``` rocketmq消费的tags值，用于过滤消息，非必填参数

+ ```from``` 自定义的数据源， 通过实现ISource接口实现自己的数据源

### transform

transform 允许在流计算过程中对输入源的数据进行修改，进行下一步的操作；DataStream API中包括```DataStream```,```JoinStream```, ```SplitStream```,```WindowStream```等多个transform类；

#### DataStream

DataStream实现了一系列常见的流计算算子

+ ```map``` 通过将源的每个记录传递给函数func来返回一个新的DataStream
+ ```flatmap``` 与map类似，一个输入项对应0个或者多个输出项
+ ```filter``` 只选择func返回true的源DStream的记录来返回一个新的DStream
+ ```forEach``` 对每个记录执行一次函数func， 返回一个新的DataStream
+ ```selectFields``` 对每个记录返回对应的字段值，返回一个新的DataStream
+ ```operate```  对每个记录执行一次自定义的函数，返回一个新的DataStream
+ ```script```  针对每个记录的字段执行一段脚本，返回新的字段，生成一个新的DataStream
+ ```toPrint``` 将结果在控制台打印，生成新的DataStreamAction实例
+ ```toFile``` 将结果保存为文件，生成一个新的DataStreamAction实例
+ ```toDB``` 将结果保存到数据库
+ ```toRocketmq``` 将结果输出到rocketmq
+ ```to``` 将结果经过自定义的ISink接口输出到指定的存储
+ ```window``` 在窗口内进行相关的统计分析，一般会与```groupBy```连用， ```window()```用来定义窗口的大小， ```groupBy()```用来定义统计分析的主key，可以指定多个
    + ```count``` 在窗口内计数
    + ```min``` 获取窗口内统计值的最小值
    + ```max``` 获取窗口内统计值得最大值
    + ```avg``` 获取窗口内统计值的平均值
    + ```sum``` 获取窗口内统计值的加和值
    + ```reduce``` 在窗口内进行自定义的汇总运算
+ ```join``` 根据条件将将俩个流进行关联， 合并为一个大流进行相关的运算
+ ```union``` 将俩个流进行合并
+ ```split``` 将一个数据流按照标签进行拆分，分为不同的数据流供下游进行分析计算
+ ```with``` with算子用来指定计算过程中的相关策略，包括checkpoint的存储策略，state的存储策略等

# Strategy

策略机制主要用来控制计算引擎运行过程中的底层逻辑，如checkpoint，state的存储方式等，后续还会增加对窗口、双流join等的控制；所有的控制策略通过```with```算子传入，可以同时传入多个策略类型；

```java
//指定checkpoint的存储策略
source
    .fromRocketmq("TSG_META_INFO","")
    .map(message->message+"--")
    .toPrint(1)
    .with(CheckpointStrategy.db("jdbc:mysql://XXXXX:3306/XXXXX","","",0L))
    .start();
```

——————————————————————————————————————————————————————————————————————————————
# RocketMQ Streams
[![Build Status](https://travis-ci.org/apache/rocketmq-streams.svg?branch=main)](https://travis-ci.com/github/apache/rocketmq-streams)
[![CodeCov](https://codecov.io/gh/apache/rocketmq-stream/branch/main/graph/badge.svg)](https://codecov.io/gh/apache/rocketmq-steams)
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://github.com/apache/rocketmq-streams/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)
## Features

* Lightweight deployment: RocketMQ Streams can be deployed separately or in cluster mode.
* Various types of data input and output: source supports RocketMQ while sink supports databases and RocketMQ, etc.

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
+ ```join```: associates the two streams according to the conditions and merges them into a large stream for related calculations.
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
——————————————
