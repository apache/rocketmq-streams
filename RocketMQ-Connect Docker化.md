# RocketMQ-Connect Docker化

### 1.RocketMQ Connect docker file 以及推到docker hub。

代码如下：

```dockerfile
FROM openjdk:11-jre-slim

# 设置工作目录
WORKDIR /rocketmq-connect

# 下载并解压 RocketMQ Connect 运行时
RUN apt-get update && \
    apt-get install -y wget && \
    wget https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=rocketmq/rocketmq-externals/rocketmq-externals-2.0.0-incubating/rocketmq-externals-2.0.0-incubating-bin-release.zip && \
    unzip rocketmq-externals-2.0.0-incubating-bin-release.zip && \
    rm rocketmq-externals-2.0.0-incubating-bin-release.zip

# 暴露端口
EXPOSE 8083

# 启动 RocketMQ Connect 运行时
CMD ["./bin/run"]
```

上面的 Dockerfile 使用 OpenJDK 11 作为基础镜像，并在其中安装了 wget。然后，它下载并解压 RocketMQ Connect 运行时，并将工作目录设置为 RocketMQ Connect 的根目录。最后，它将端口 8083 暴露给外部，并启动 RocketMQ Connect 运行时。

要构建 Docker 镜像，你可以使用以下命令：

```
docker build -t your-image-name .
```

将 `your-image-name` 替换为你想要为镜像设置的名称。构建完成后，你可以使用以下命令将镜像推送到 Docker Hub：

```
docker push your-image-name
```

这将把你的镜像推送到 Docker Hub 中，以便其他人可以轻松地下载和使用它。

### 2.提供一键启动的脚本，帮助快速拉起一个RocketMQ Connect 集群，包括内置的File demo

以下是一个Bash 脚本，可以帮助你快速启动一个RocketMQ Connect 集群，并包含内置的File demo：

```bash
#!/bin/bash

# 拉取镜像
docker pull your-image-name

# 启动 ZooKeeper
docker run -d --name zookeeper zookeeper:3.5

# 启动 NameServer
docker run -d --name namesrv -e "ROCKETMQ_CONFIG_NAMESRV_ADDR=127.0.0.1:9876"  rocketmqinc/rocketmq:4.9.0 sh mqnamesrv

# 启动 Broker
docker run -d --name broker -p 10911:10911 -p 10909:10909 \
    -e "ROCKETMQ_NAMESRV_ADDR=127.0.0.1:9876" \
    -e "JAVA_OPTS=-Duser.home=/opt" \
    rocketmqinc/rocketmq:4.9.0 sh mqbroker -c /opt/rocketmq-4.9.0/conf/2m-noslave/broker.conf

# 启动 RocketMQ Connect
docker run -d --name rocketmq-connect -p 8083:8083 \
    -e "NAMESRV_ADDR=127.0.0.1:9876" \
    -e "GROUP_ID=rocketmq-connect-group" \
    -e "CONFIG_FILE=/rocketmq-connect/config/rocketmq.properties" \
    -v "$(pwd)/config:/rocketmq-connect/config" \
    -v "$(pwd)/data:/rocketmq-connect/data" \
    your-image-name

# 启动内置的 File demo
docker exec -it rocketmq-connect bash -c "cd /rocketmq-externals/rocketmq-example && sh bin/run.sh"
```

上面的脚本假设你已经构建并推送了一个名为 `your-image-name` 的 Docker 镜像。脚本首先拉取该镜像，然后启动 ZooKeeper、NameServer 和 Broker。接下来，它启动 RocketMQ Connect 容器，并将配置文件和数据目录挂载到本地目录中。最后，它在 RocketMQ Connect 容器中执行内置的 File demo。

### 3.提供Quick start 文档，帮助新用户快速启动

以下是 RocketMQ Connect 的快速入门指南，帮助新用户快速启动 RocketMQ Connect。

#### 步骤一：安装 Docker

首先，在计算机上安装 Docker，以便能够运行 RocketMQ Connect 容器。可以从 Docker 的官方网站下载安装程序，然后按照安装向导的指示进行安装。

#### 步骤二：下载 RocketMQ Connect 运行时

接下来，下载 RocketMQ Connect 运行时。可以从 [Apache RocketMQ 官网](https://rocketmq.apache.org/) 下载最新版本的 RocketMQ Connect。

#### 步骤三：构建 Docker 镜像

一旦下载了 RocketMQ Connect 运行时，就可以使用 Dockerfile 构建 Docker 镜像。可以使用以下命令：

```
docker build -t your-image-name .
```

将 `your-image-name` 替换为你想要为镜像设置的名称。

#### 步骤四：启动 RocketMQ Connect

一旦你构建了 Docker 镜像，就可以使用以下命令启动 RocketMQ Connect：

```
docker run -d --name rocketmq-connect -p 8083:8083 \
    -e "NAMESRV_ADDR=127.0.0.1:9876" \
    -e "GROUP_ID=rocketmq-connect-group" \
    -e "CONFIG_FILE=/rocketmq-connect/config/rocketmq.properties" \
    -v "$(pwd)/config:/rocketmq-connect/config" \
    -v "$(pwd)/data:/rocketmq-connect/data" \
    your-image-name
```

这将启动一个名为 rocketmq-connect 的容器，并将其绑定到本地端口 8083 上。它还将设置 NameServer 的地址、RocketMQ Connect 的 Group ID，并指定 RocketMQ Connect 的配置文件位置和数据目录位置。

#### 步骤五：启动内置的 File demo

还可以在 RocketMQ Connect 容器中启动内置的 File demo。可以使用以下命令：

```
docker exec -it rocketmq-connect bash -c "cd /rocketmq-externals/rocketmq-example && sh bin/run.sh"
```

这将在容器中启动一个名为 rocketmq-example 的内置示例应用程序。

现在，你已经成功启动了 RocketMQ Connect。你可以使用你的喜爱的编程语言编写自己的 RocketMQ Connect 插件，并将它们部署到 RocketMQ Connect 运行时中。如果你需要更多详细信息，请查阅 RocketMQ Connect 的官方文档。