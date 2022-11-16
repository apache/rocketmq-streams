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

public class Constant {

    public static final String SHUFFLE_KEY_CLASS_NAME = "shuffle.key.class.name";

    public static final String SHUFFLE_VALUE_CLASS_NAME = "shuffle.value.class.name";

    public final static String STATE_TOPIC_SUFFIX = "-stateTopic";

    public static final String SHUFFLE_TOPIC_SUFFIX = "-shuffleTopic";

    public static final String FIRED_TIME_PREFIX = "fired-time-";

    public static final String TIME_TYPE = "timeType";

    public static final String ALLOW_LATENESS_MILLISECOND = "allowLatenessMillisecond";

    public static final String SPLIT = "@";

    public static final String EMPTY_BODY = "empty_body";

    public static final String TRUE = "true";

    public static final String SOURCE_TIMESTAMP = "source_timestamp";

    public static final String STREAM_TAG = "stream_tag";

    public static final String WINDOW_FOR_JOIN = "window_for_join";

    public static final String WINDOW_JOIN_TYPE = "window_join_type";

    public static final String JOIN_STREAM_SIDE = "join_stream_side";
    public static final String JOIN_COMMON_SHUFFLE_TOPIC = "join_common_shuffle_topic";

    public static final String COMMON_NAME_MAKER = "common_name_maker";

    public static final String WINDOW_START_TIME = "window_start_time";

    public static final String WINDOW_END_TIME = "window_end_time";

}
