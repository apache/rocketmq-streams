# RocketMQ Streams [![Build Status](https://app.travis-ci.com/apache/rocketmq-streams.svg?branch=main)](https://app.travis-ci.com/apache/rocketmq-streams) [![CodeCov](https://codecov.io/gh/apache/rocketmq-stream/branch/main/graph/badge.svg)](https://app.codecov.io/gh/apache/rocketmq-streams)

[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://github.com/apache/rocketmq-streams/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)

## [中文文档](./README-Chinese.md)
## [Quick Start](./quick_start.md)


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
