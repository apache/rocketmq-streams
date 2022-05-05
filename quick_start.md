# 快速开发

## 引入相关的jar包

```xml

<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-streams-clients</artifactId>
</dependency>

```

## 开发实时应用程序

```java

public class RocketmqExample {

    public static void main(String[] args) {

        DataStreamSource dataStream = StreamBuilder.dataStream("test_namespace", "graph_pipeline");

        dataStream
            .fromFile("data.csv", false)   //构建实时任务的数据源
            .map(message -> message.split(","))   //构建实时任务处理的逻辑过程
            .toPrint(1)   //构建实时任务的输出
            .start();    //启动实时任务
    }
}

```

## 运行

打包

```shell
mvn -Prelease-all -DskipTests clean install -U
```

运行

```shell
 java -jar jarName mainClass
```
