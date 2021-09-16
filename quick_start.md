## rocketmq-streams 快速搭建
---
### 前言
本文档主要介绍如何基于rocketmq-streams快熟搭建流处理任务，搭建过程中某些例子会用到rocketmq，可以参考[rocketmq搭建文档见文档](https://rocketmq.apache.org/docs/quick-start/)


### 1、源码构建

#### 1.1、构建环境
 - JDK 1.8 and above
 - Maven 3.2 and above

#### 1.2、构建Rocketmq-streams

`git clone https://github.com/apache/rocketmq-streams.git`
`cd rocketmq-streams`
`mvn clean -DskipTests  install -U`


### 2、基于rocketmq-streams创建应用

#### 2.1、pom依赖
```xml
<dependency>
    <groupId>org.apache.rocketmq</groupId>
    <artifactId>rocketmq-streams-clients</artifactId>
</dependency>
```
#### 2.2、shade clients依赖包
```xml
    <build>
        <plugins>
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
        </plugins>
    </build>
```

#### 2.3、编写业务代码
Please see the [rocketmq-streams-examples](rocketmq-streams-examples/README.md)
#### 2.4、运行
- 前提：在从rocketmq中读取数据做流处理时，需要运行topic在rocketmq中自动创建，因为做groupBy操作时，需要用到rocketmq作为shuffle数据的读写目的地。
- 命令：
```
   java -jar XXXX-shade.jar \ 
        -Dlog4j.level=ERROR \
        -Dlog4j.home=/logs  \
        -Xms1024m \
        -Xmx1024m 
```

