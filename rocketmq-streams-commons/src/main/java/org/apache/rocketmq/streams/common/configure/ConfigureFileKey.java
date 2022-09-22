/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.common.configure;

import org.apache.rocketmq.streams.common.component.ComponentCreator;

/**
 * 配置文件中key config of dipper server
 */
public interface ConfigureFileKey {

    String CONNECT_TYPE = "configurable.storage.type";
    /**
     * 数据库url
     */
    String DB_TYPE = "jdbc.type";
    String JDBC_URL = "jdbc.url";
    String JDBC_USERNAME = "jdbc.username";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_TABLE_NAME = "table.name";
    String SECRECY = "configure.sec.key";
    String SECRECY_DEFAULT = "";

    String DIPPER_REGEX_ENGINE = "regex.engine.option";

    /**
     * whether to start engine when loading configurable operator, true(default) or false
     */
    String DIPPER_RUNNING_STATUS = "running.status";

    String DIPPER_RUNNING_STATUS_DEFAULT = "true";


    String POLLING_TIME = "configurable.polling.time";

    /**
     * 默认的文件存储transport的name
     */
    String FILE_TRANSPORT_NAME = "file.transport.name";
    String FILE_TRANSPORT_AK = "file.transport.ak";
    String FILE_TRANSPORT_SK = "file.transport.sk";
    String FILE_TRANSPORT_ENDPOINT = "file.transport.endpoint";
    String FILE_TRANSPORT_DIPPER_DIR = "file.transport.dir";

    /**
     * 监控相关的配置，监控输入的级别，有三种：INFO,SLOW,ERROR
     */
    String MONITOR_OUTPUT_LEVEL = "monitor.output.level";
    /**
     * 超过多长时间，输出慢查询
     */
    String MONITOR_SLOW_TIMEOUT = "monitor.slow.timeout";
    /**
     * 如果是日志输出，需要指定默认的目录
     */
    String MONTIOR_LOGGER_DIR = "monitor.loger.dir";

    /**
     * 是否把原始日志备份下来，以备做效果测试
     */
    String INNER_MESSAGE_SWITCH = "inner.message.save.switch";
    /**
     * 是否在做数据回放，如果再做数据回放，自动判读结果是否符合预期
     */
    String DIPPER_ORIG_MESSAGE_PLAYBACK = "orimessage.playback";

    //dipper自己实现的延迟队列
    String DIPPER_WINDOW_DELAY_CHANNEL_TOPIC = "window.delay.channel.topic";

    //join的默认窗口大小
    String DIPPER_WINDOW_JOIN_DEFAULT_ITERVA_SIZE = "window.join.default.iterval.size.time";
    //    //join需要保留几个窗口，3个窗口意味着join的范围是上下15分钟多
    String DIPPER_WINDOW_JOIN_RETAIN_WINDOW_COUNT = "window.join.default.retain.window.count";
    //窗口多长延迟多长时间触发，确保多台机器的数据写入存储
    String DIPPER_WINDOW_DEFAULT_FIRE_DELAY_SECOND = "window.default.fire.delay.second";
    //统计默认的窗口大小，单位是分钟。默认是滚动窗口，大小是1个小时
    String DIPPER_WINDOW_DEFAULT_INERVAL_SIZE = "window.default.iterval.size.time";
    //统计默认的窗口大小，单位是分钟。默认是滚动窗口，大小是1个小时
    String DIPPER_WINDOW_DEFAULT_TIME_UNIT_ADJUST = "window.default.time.unit.adjust";
    //over partition窗口的默认时间
    String DIPPER_WINDOW_OVER_DEFAULT_ITERVA_SIZE = "window.over.default.iterval.size.time";

    static String getDipperJdbcUrl() {
        return ComponentCreator.getProperties().getProperty(JDBC_URL);
    }

    static String getDipperJdbcUserName() {
        return ComponentCreator.getProperties().getProperty(JDBC_USERNAME);
    }

    static String getDipperJdbcPassword() {
        return ComponentCreator.getProperties().getProperty(JDBC_PASSWORD);
    }

    /**
     * shuffle相关配置，如果后面加上.namespace,则只对某个namespace生效，如window.shuffle.channel.type.namespace=rocketmq,相当于只对这个namespace配置
     */
    String WINDOW_SHUFFLE_CHANNEL_TYPE = "window.shuffle.channel.type";//window 做shuffle中转需要的消息队列类型
    //比如rocketmq，需要topic，tags和group,属性值和字段名保持一致即可。配置如下:window.shuffle.channel.topic=abdc    window.shuffle.channel.tag=fdd

    String WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX = "window.shuffle.channel.";
    String WINDOW_SYSTEM_MESSAGE_CHENNEL_OWNER = "window.system.message.channel.owner";//如果能做消息过滤，只过滤本window的消息，可以配置这个属性，如rocketmq的tags.不支持的会做客户端过滤
    /**
     * 通知相关
     */
    String WINDOW_SYSTEM_MESSAGE_CHENNEL_TYPE = "window.system.message.channel.type";
    String WINDOW_SYSTEM_MESSAGE_CHENNEL_PROPERTY_PREFIX = "window.system.message.channel.";

    String LEASE_STORAGE_NAME = "DB";//通过这个配置，可以修改lease 的底层存储

    String CHECKPOINT_STORAGE_NAME = "checkPointStorageName";

    String IS_RECOVER_MODE = "isRecover";

    String IS_ATOMIC_DB_SINK = "isAtomicDbSink";

    /**
     * minibatch
     */

    String WINDOW_MINIBATCH_SWITCH="window.minibatch.switch";//是否启动minibatch

}
