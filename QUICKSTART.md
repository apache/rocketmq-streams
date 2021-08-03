# Quick Start

本文档详细介绍了如何在rocketmq上执行流计算任务；

## 所需环境

+ 64bit OS, Linux/Unix/Mac is recommended;(Windows user see guide below)
+ 64bit JDK 1.8+;
+ Maven 3.2.X

## 使用步骤

### 1. 创建maven项目， 并依赖rocketmq-streams的客户端

```xml

<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-streams-clients</artifactId>
    <version>2.0.0-SNAPSHOT</version>
</dependency>
```

### 2. 在主函数中按照streams的开发规范，编写业务逻辑

```java
import org.apache.rocketmq.streams.client.transform.DataStream;

public static void main(String[]args){
    DataStreamSource source=StreamBuilder.dataStream("namespace","pipeline");

    source
    .fromFile("～/admin/data/text.txt",false)
    .map(message->message)
    .toPrint(1)
    .start();
    }
```

### 3. 在pom.xml中加入shade插件， 将依赖的stream与业务代码一并打包， 形成-shaded.jar 包

```xml

<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>3.2.1</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <minimizeJar>false</minimizeJar>
                <shadedArtifactAttached>true</shadedArtifactAttached>
                <artifactSet>
                    <includes>
                        <include>org.apache.rocketmq:rocketmq-streams-clients</include>
                    </includes>
                </artifactSet>
            </configuration>
        </execution>
    </executions>
</plugin>

```

### 4. 将jar包拷贝到应用服务器，作为普通的java应用直接运行

```
   java -jar XXXX-shade.jar \ 
        -Dlog4j.level=ERROR \
        -Dlog4j.home=/logs  \
        -Xms1024m \
        -Xmx1024m 
```




