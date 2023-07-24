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


package org.apache.rocketmq.streams.core.common;

import java.nio.charset.StandardCharsets;

public class Constant {

    public static final String SHUFFLE_KEY_CLASS_NAME = "shuffle.key.class.name";

    public static final String SHUFFLE_VALUE_CLASS_NAME = "shuffle.value.class.name";

    public final static String STATE_TOPIC_SUFFIX = "-stateTopic";

    public static final String SHUFFLE_TOPIC_SUFFIX = "-shuffleTopic";

    public static final String SKIP_DATA_ERROR = "skip_data_error";

    public static final String SPLIT = "@";

    public static final String EMPTY_BODY = "empty_body";

    public static final String TRUE = "true";

    public static final String SOURCE_TIMESTAMP = "source_timestamp";

    public static final String STREAM_TAG = "stream_tag";

    public static final String WINDOW_START_TIME = "window_start_time";

    public static final String WINDOW_END_TIME = "window_end_time";

    public static final String WORKER_THREAD_NAME = "ROCKETMQ_STREAMS";

    public static final String STATIC_TOPIC_BROKER_NAME = "__syslo__global__";

    public static final String WATERMARK_KEY = "watermark_key";

    public static final Integer MAX_RETRY_SEEK_TIMES = 3;

    public static final Integer MAX_RETRY_WAIT_TIMES = 3;


    public static final Long WAIT_STEP = 1000L;
}
