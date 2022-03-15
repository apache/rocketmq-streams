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
package org.apache.rocketmq.streams.common.monitor;

public class DataSyncConstants {

    /**
     * rocketmq-stream更新模块对应的topic
     */
    public static final String RULE_UP_TOPIC = "dipper.console.topic.up";

    /**
     * rocketmq-stream更新模块对应的topic
     */
    public static final String RULE_DOWN_TOPIC = "dipper.console.topic.down";

    /**
     * rocketmq-stream更新模块对应的tag
     */
    public static final String RULE_TOPIC_TAG = "dipper.console.tag";

    /**
     * rocketmq-stream更新模块对应的消息渠道类型 默认为metaq
     */
    public static final String UPDATE_TYPE = "dipper.console.service.type";

    public static final String UPDATE_TYPE_HTTP = "http";
    public static final String UPDATE_TYPE_DB = "db";
    public static final String UPDATE_TYPE_ROCKETMQ = "rocketmq";
}
