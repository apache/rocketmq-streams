# 数据处理逻辑

## map

```java

dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .map(message->message+"~~~~~")
    .toPrint()
    .start();

```

## flatmap

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .toPrint()
    .start();
```

## filter

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .filter(message->message.contains("xxxxx")) //为true时数据继续向下游输出，否则别拦截
    .toPrint()
    .start();

```

## forEach

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .forEach(message->message.contains("xxxxx")) //为true时数据继续向下游输出，否则别拦截
    .toPrint()
    .start();

```

## selectFields

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .forEach(message->message.contains("xxxxx")) //为true时数据继续向下游输出，否则别拦截
    .selectFields("field1","field2")
    .toPrint()
    .start();

```

## script

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .script("ProjectName, =, project") //为true时数据继续向下游输出，否则别拦截
    .toPrint()
    .start();

```

## Window

在窗口内进行相关的统计分析，一般会与```groupBy```连用， ```window()```用来定义窗口的大小， ```groupBy()```用来定义统计分析的主key，可以指定多个

### count

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .window(TumblingWindow.of(Time.minutes(1)))
    .groupBy("AttributeCode")
    .count("asName") //指定别名
    .toDataSteam()
    .toPrint()
    .start();

```

### avg

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .window(TumblingWindow.of(Time.minutes(1)))
    .groupBy("AttributeCode")
    .avg("field","avg_value")
    .toDataSteam()
    .toPrint()
    .start();

```

### min

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .window(TumblingWindow.of(Time.minutes(1)))
    .groupBy("AttributeCode")
    .min("field")
    .toDataSteam()
    .toPrint()
    .start();

```

### max

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .window(TumblingWindow.of(Time.minutes(1)))
    .groupBy("AttributeCode")
    .max("field")
    .toDataSteam()
    .toPrint()
    .start();

```

### sum

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .window(TumblingWindow.of(Time.minutes(1)))
    .groupBy("AttributeCode")
    .sum("field","asField")
    .toDataSteam()
    .toPrint()
    .start();

```

### reduce

```java
dataStream.fromMqtt("xxxxx","xxxx","xxxxxx","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .window(TumblingWindow.of(Time.minutes(1)))
    .groupBy("AttributeCode")
    .ruduce(new ReduceFunction(){})
    .toDataSteam()
    .toPrint()
    .start();

```

## Join

关键计算，根据条件将俩个流，或者流与物理表进行关联，最终输出结果

### join

根据条件将俩个流进行内关联

```java
    DataStream left=......;
    DataStream right=......;
    left.join(right).on("(ProjectName,=,project)").toDataSteam().toPrint().start();
```

### leftJoin

根据条件将俩个流的数据进行左关联

```java
    DataStream left=......;
    DataStream right=......;
    left.leftJoin(right).on("(ProjectName,=,project)").toDataSteam().toPrint().start();
```

### dimJoin

根据条件将流与维表进行内关联，维表的数据可以来自于文件，也可以来自于数据库

```java

DataStream dataStream=......;

    dataStream
    .dimJoin("classpath://dim.txt",10000)
    .on("(ProjectName,=,project)")
    .toDataSteam()
    .toPrint()
    .start();
```

### dimLeftJoin

根据条件将流与维表进行左关联，维表的数据可以来自于文件，也可以来自于数据库

```java

DataStream dataStream=......;

    dataStream
    .dimLeftJoin("classpath://dim.txt",10000)
    .on("(ProjectName,=,project)")
    .toDataSteam()
    .toPrint()
    .start();
```

## Union

将俩个流进行合并

```java
    DataStream leftStream=......;
    DataStream rightStream=......;
    leftStream.union(rightStream).toPrint().start();
```

## Split

将一个数据流按照标签进行拆分，分为不同的数据流供下游进行分析计算

```java
    DataStream dataStream=......;
    stream.split(new SplitFunction<JSONObject>(){}).toPrint().start();
```

## with

with算子用来指定计算过程中的相关策略，包括checkpoint的存储策略，state的存储策略等

```java
   dataStream.fromMqtt("","","","","")
    .flatMap(message->((JSONObject)message).getJSONArray("Data"))
    .window(TumblingWindow.of(Time.minutes(1)))
    .groupBy("AttributeCode")
    .setLocalStorageOnly(true)
    .avg("Value","avg_value")
    .toDataSteam()
    .toPrint()
    .with(ShuffleStrategy.shuffleWithMemory())
    .start();
```