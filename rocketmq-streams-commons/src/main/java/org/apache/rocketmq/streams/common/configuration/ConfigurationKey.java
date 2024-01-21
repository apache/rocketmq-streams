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
package org.apache.rocketmq.streams.common.configuration;

/**
 * 配置文件中key config of dipper server
 */
public class ConfigurationKey {

    /**
     * 系统参数： 数据库url, 主要用于租约
     */
    public final static String JDBC_URL = "dipper.rds.jdbc.url";
    public final static String JDBC_USERNAME = "dipper.rds.jdbc.username";
    public final static String JDBC_PASSWORD = "dipper.rds.jdbc.password";
    public final static String JDBC_DRIVER = "dipper.rds.jdbc.driver";
    /**
     * 系统参数： 租约的续约周期
     */
    public final static String DIPPER_DISPATCHER_SCHEDULE_TIME = "dipper.dispatcher.schedule.time";

    /**
     * 系统参数： 情报的连接信息，只在专有云使用
     */
    public final static String INTELLIGENCE_JDBC_URL = "intelligence.rds.jdbc.url";
    public final static String INTELLIGENCE_JDBC_USERNAME = "intelligence.rds.jdbc.username";
    public final static String INTELLIGENCE_JDBC_PASSWORD = "intelligence.rds.jdbc.password";
    public final static String INTELLIGENCE_SWITCH = "intelligence.switch.open";
    public final static String INTELLIGENCE_TIP_DB_ENDPOINT = "intelligence.tip.db.endpoint";
    public final static String INTELLIGENCE_AK = "intelligence_ak";
    public final static String INTELLIGENCE_SK = "intelligence_sk";
    public final static String INTELLIGENCE_REGION = "intelligence_region";

    /*
     * 系统参数： 正则引擎的选择，有hyperscan 和 re2j俩种
     */
    public final static String DIPPER_REGEX_ENGINE = "dipper.regex.engine.option";

    /**
     * 系统参数： 超过多长时间，输出慢查询
     */
    public final static String MONITOR_SLOW_TIMEOUT = "dipper.monitor.slow.timeout";
    public final static String MONITOR_PIPELINE_HTML_SWITCH = "dipper.monitor.pipeline.html.switch";
    /**
     * 系统参数： join 默认join的时间，单位是秒，默认是5分钟
     */
    public final static String DIPPER_WINDOW_JOIN_DEFAULT_INTERVAL_SIZE = "dipper.window.join.default.interval.size.time";
    /**
     * 系统参数：join需要保留几个窗口，3个窗口意味着join的范围是上下15分钟多
     */
    public final static String DIPPER_WINDOW_JOIN_RETAIN_WINDOW_COUNT = "dipper.window.join.default.retain.window.count";

    /**
     * 系统参数：统计默认的窗口大小，单位是秒。默认是滚动窗口，默认是10分钟
     */
    public final static String DIPPER_WINDOW_DEFAULT_INTERVAL_SIZE = "dipper.window.default.interval.size.time";

    /**
     * 系统参数：over partition窗口的默认时间
     */

    public final static String DIPPER_WINDOW_OVER_DEFAULT_INTERVAL_SIZE = "dipper.window.over.default.interval.size.time";
    public final static String DIPPER_WINDOW_OVER_DEFAULT_EMIT_BEFORE_SECOND = "dipper.window.over.default.emit.before.size.second";

    /**
     * shuffle相关配置，如果后面加上.namespace,则只对某个namespace生效，如window.shuffle.channel.type.namespace=rocketmq,相当于只对这个namespace配置
     */
    public final static String WINDOW_SHUFFLE_CHANNEL_TYPE = "window.shuffle.channel.type";//window 做shuffle中转需要的消息队列类型

    public final static String WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX = "window.shuffle.channel.";
    /**
     * 系统参数：是否启动mini batch
     */
    public final static String WINDOW_MINI_BATCH_SWITCH = "window.mini.batch.switch";

    /**
     * 系统参数：是否是本地存储
     */
    public final static String WINDOW_STORAGE_IS_LOCAL = "window.storage.is.local";

    /**
     * sql manager使用，选择sql的存储方式
     */
    public final static String SQL_STORAGE_TYPE = "sql.storage.type";

    public final static String FINGERPRINT_CACHE_SIZE = "fingerprint.cache.size";

    /**
     * 默认的文件存储transport的name
     */
    public final static String FILE_TRANSPORT_AK = "dipper.file.transport.ak";
    public final static String FILE_TRANSPORT_SK = "dipper.file.transport.sk";
    public final static String FILE_TRANSPORT_ENDPOINT = "dipper.file.transport.endpoint";
    public final static String FILE_TRANSPORT_DIPPER_DIR = "dipper.file.transport.dipper.dir";

    public final static String LEASE_STORAGE_NAME = "lease.storage.name";//通过这个配置，可以修改lease 的底层存储

    //window操作系统执行python脚本时的配置
    public final static String PYTHON_WORK_DIR = "siem.python.workdir";

    /**
     * rocketmq-stream更新模块对应的topic
     */
    public final static String RULE_UP_TOPIC = "dipper.console.topic.up";

    /**
     * rocketmq-stream更新模块对应的topic
     */
    public final static String RULE_UP_TAG = "dipper.console.topic.up.tags";

    /**
     * rocketmq-stream更新模块对应的topic
     */
    public final static String RULE_DOWN_TOPIC = "dipper.console.topic.down";
    /**
     * rocketmq-stream更新模块对应的topic
     */
    public final static String RULE_DOWN_TAG = "dipper.console.topic.down.tags";
    /**
     * rocketmq-stream更新模块对应的tag
     */
    public final static String RULE_TOPIC_TAG = "dipper.console.tag";
    /**
     * rocketmq-stream更新模块对应的消息渠道类型 默认为metaq
     */
    public final static String UPDATE_TYPE = "dipper.console.service.type";
    public final static String HTTP_SERVICE_ENDPOINT = "dipper.configurable.service.type.http.endpoint";
    public final static String SYSLOG_TCP_PORT_PROPERTY_KEY = "dipper.syslog.server.tcp.port";//当需要改变端口值时，通过配置文件增加dipper.syslog.server.tcp.port=新端口的值
    public final static String SYSLOG_UDP_PORT_PROPERTY_KEY = "dipper.syslog.server.udp.port";//当需要改变端口值时，通过配置文件增加dipper.syslog.server.tcp.port=新端口的值
    public final static String ENV_JDBC_URL = "rocketmq_streams_sync_jdbc_url";
    public final static String ENV_JDBC_USERNAME = "rocketmq_streams_sync_jdbc_username";
    public final static String ENV_JDBC_PASSWORD = "rocketmq_streams_sync_jdbc_password";
    public final static String ENV_JDBC_DRIVER = "rocketmq_streams_sync_jdbc_driver";
    public final static String HTTP_AK = "rocketmq.streams.channel.ak";
    public final static String HTTP_SK = "rocketmq.streams.channel.sk";

    public final static String DEFAULT_JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    public final static String DEFAULT_JDBC_TABLE_NAME = "rocketmq_streams_checkpoint_table";

    public final static String BASELINE_ENTITY_CACHE_SIZE = "dipper.baseline.entity.cache.size";//基线中缓存条数
    public final static String RUNTIME_ENV = "dipper.runtime.env";

}
