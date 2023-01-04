# RocketMQ Streams 
[![Build Status](https://app.travis-ci.com/apache/rocketmq-streams.svg?branch=main)](https://app.travis-ci.com/apache/rocketmq-streams)
[![CodeCov](https://codecov.io/gh/apache/rocketmq-stream/branch/main/graph/badge.svg)](https://app.codecov.io/gh/apache/rocketmq-streams) 
[![GitHub release](https://img.shields.io/badge/release-download-orange.svg)](https://github.com/apache/rocketmq-streams/releases)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Average time to resolve an issue](http://isitmaintained.com/badge/resolution/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Average time to resolve an issue")
[![Percentage of issues still open](http://isitmaintained.com/badge/open/apache/rocketmq-streams.svg)](http://isitmaintained.com/project/apache/rocketmq-streams "Percentage of issues still open")
[![Twitter Follow](https://img.shields.io/twitter/follow/ApacheRocketMQ?style=social)](https://twitter.com/intent/follow?screen_name=ApacheRocketMQ)


**RocketMQ Streams is a lightweight stream processing framework, application gains the stream processing ability by depending on RocketMQ Streams as an SDK.**

It offers a variety of features:

* Function:
  * One-to-one transform function, such as: filter, map, foreach
  * Aggregate function, such as: sum, min, max, count, aggregate
  * Generating function, such as: flatMap
* Group by aggregate and window aggregate
* Join stream
* Custom serialization 
----------

## Quick Start
This paragraph guides you running a stream processing with RocketMQ Streams.

### Run RocketMQ 5.0 locally 
[RocketMQ quick-start](https://rocketmq.apache.org/docs/quick-start/)

RocketMQ runs on all major operating systems and requires only a Java JDK version 8 or higher to be installed.
To check, run `java -version`:
```shell
$ java -version
java version "1.8.0_121"
```
**1) Download RocketMQ**
```shell
wget https://archive.apache.org/dist/rocketmq/5.0.0/rocketmq-all-5.0.0-bin-release.zip

# Unpack the release
$ unzip rocketmq-all-5.0.0-bin-release.zip

$ cd rocketmq-all-5.0.0-bin-release/bin
```

**2) Start NameServer**

NameServer will be listening at `0.0.0.0:9876`, make sure that the port is not used by others on the local machine, and then do as follows.

```shell
### start Name Server
$ nohup sh mqnamesrv &

### check whether Name Server is successfully started
$ tail -f ~/logs/rocketmqlogs/namesrv.log
The Name Server boot success...
```

**2) Start Broker**

```shell
### start Broker
$ nohup sh bin/mqbroker -n localhost:9876 &

### check whether Broker is successfully started, eg: Broker's IP is 192.168.1.2, Broker's name is broker-a
$ tail -f ~/logs/rocketmqlogs/broker.log
The broker[broker-a, 192.169.1.2:10911] boot success...
```

### Build stream processing application

**1) Build application in IDE**

**2) Add RocketMQ Streams dependency**
```xml
    <dependency>
        <groupId>org.apache.rocketmq</groupId>
        <artifactId>rocketmq-streams</artifactId>
        <version>{current.version}</version>
    </dependency>
```

**3) Build stream processing application**

* create topic in RocketMQ before start the stream processing.
```shell
sh bin/mqadmin updateTopic -c ${clusterName} -t ${topicName} -r 8 -w 8 -n 127.0.0.1:9876
```
    
    NOTE: the default clusterName is DefaultCluster in this quick-start doc, changes it with your RocketMQ cluster.

* add your stream processing code, The following is an example. more examples are [here](./example/src/main/java/org/apache/rocketmq/streams/examples).
```java
public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder("wordCount");

        builder.source("sourceTopic",  total -> {
                    String value = new String(total, StandardCharsets.UTF_8);
                    return new Pair<>(null, value);
                })
                .flatMap((ValueMapperAction<String, List<String>>) value -> {
                    String[] splits = value.toLowerCase().split("\\W+");
                    return Arrays.asList(splits);
                })
                .keyBy(value -> value)
                .count()
                .toRStream()
                .print();

        TopologyBuilder topologyBuilder = builder.build();

        Properties properties = new Properties();
        properties.put(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");

        RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(new Thread("wordcount-shutdown-hook") {
            @Override
            public void run() {
                rocketMQStream.stop();
                latch.countDown();
            }
        });

        try {
            rocketMQStream.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
```
