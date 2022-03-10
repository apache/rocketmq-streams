[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://github.com/apache/rocketmq-streams/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)

# Features

* 轻量级部署：可以单独部署，也支持集群部署
* 多种类型的数据输入以及输出，source 支持 rocketmq ， sink 支持db, rocketmq 等

# Architecture
- [整体架构](docs/design/1.RocketMQ-streams整体架构.md)： 介绍RocketMQ-streams总体构成；
  
- [构建拓扑图](docs/design/2.构建DataStream.md)： 介绍如何通过DataStream将各个算子添加进入计算拓扑；

- [启动](docs/design/3.启动DataStream.md)：介绍计算拓扑图如何启动，消费数据流入之前需要做那些准备工作；

- [数据的流转](docs/design/4.数据的流转过程.md)：介绍数据进入计算拓扑的流转过程，以及各种系统消息的作用原理；

- [Window算子解析](docs/design/5.Window算子解析.md)： 介绍有状态算子window实例化、数据处理、窗口触发过程；


# DataStream Example

```java
import org.apache.rocketmq.streams.client.transform.DataStream;

DataStreamSource source=StreamBuilder.dataStream("namespace","pipeline");

    source
    .fromFile("～/admin/data/text.txt",false)
    .map(message->message)
    .toPrint(1)
    .start();
```

# Maven Repository

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

StreamBuilder 用于构建流任务的源；

+ [dataStream(nameSpaceName,pipelineName)]() 返回DataStreamSource实例，用于分段编程实现流计算任务；

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

+ ```fromMqtt``` 从满足MQTT协议的终端读取数据， 满足边缘计算的场景，其中包含9个参数
    + ```url```  mqtt broker的地址，必填参数
    + ```clientId``` 客户端ID， 必填参数，相同的clientId有负载的作用
    + ```topic``` topic信息， 必填参数
    + ```username``` 用户名, 非必填，在mqtt端添加鉴权机制时使用
    + ```password``` 密码，非必填参数，在mqtt端添加鉴权机制时使用
    + ```cleanSession``` 是否清理session信息， 非必填，默认为true
    + ```connectionTimeout``` 连接超时信息， 非必填，默认是10s
    + ```aliveInterval```  判断连接是否活跃的间隔时间，非必填，默认是60s
    + ```automaticReconnect``` 连接断开后自动重连机制，非必填，默认是true


+ ```from``` 自定义的数据源， 通过实现ISource接口实现自己的数据源

### transform

transform 允许在流计算过程中对输入源的数据进行修改，进行下一步的操作；DataStream API中包括```DataStream```,```JoinStream```, ```SplitStream```,```WindowStream```等多个transform类；

DataStream实现了一系列常见的流计算算子

+ ```map``` 通过将源的每个记录传递给函数func来返回一个新的DataStream
+ ```flatmap``` 与map类似，一个输入项对应0个或者多个输出项
+ ```filter``` 只选择func返回true的源DStream的记录来返回一个新的DStream
+ ```forEach``` 对每个记录执行一次函数func， 返回一个新的DataStream
+ ```selectFields``` 对每个记录返回对应的字段值，返回一个新的DataStream
+ ```operate```  对每个记录执行一次自定义的函数，返回一个新的DataStream
+ ```script```  针对每个记录的字段执行一段脚本，返回新的字段，生成一个新的DataStream
+ ```toPrint``` 将结果在控制台打印，生成新的DataStream实例
+ ```toFile``` 将结果保存为文件，生成一个新的DataStream实例
+ ```toMqtt``` 将结果输出到满足mqtt协议的设备中，生成一个新的DataStream实例
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
+ ```join``` 根据条件将俩个流进行内关联
+ ```leftJoin``` 根据条件将俩个流的数据进行左关联
+ ```dimJoin``` 根据条件将流与维表进行内关联，维表的数据可以来自于文件，也可以来自于数据库
+ ```dimLeftJoin``` 根据条件将流与维表进行左关联，维表的数据可以来自于文件，也可以来自于数据库
+ ```union``` 将俩个流进行合并
+ ```split``` 将一个数据流按照标签进行拆分，分为不同的数据流供下游进行分析计算
+ ```with``` with算子用来指定计算过程中的相关策略，包括checkpoint的存储策略，state的存储策略等

#### Strategy

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

# 运行

Rocketmq-Streams 作为典型的java应用，既可以集成在业务系统里运行，也可以作为一个独立的jar包来运行；

首先对应用的源码进行编译

```shell
mvn -Prelease-all -DskipTests clean install -U
```

然后直接通过java指令来运行

```shell
 java -jar jarName mainClass
```

更多详细的案例可以看[这里](docs/SUMMARY.md)