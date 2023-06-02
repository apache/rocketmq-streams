package org.apache.rocketmq.streams.core.metadata;
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

public class StreamConfig {
    public static final String TIME_TYPE = "timeType";
    public static final String ALLOW_LATENESS_MILLISECOND = "allowLatenessMillisecond";
    public static final String IDLE_TIME_TO_FIRE_WINDOW = "idleTimeToFireWindow";
    public static final String ROCKETMQ_STREAMS_CONSUMER_GROUP = "__source_shuffle_group";
    public static final String ROCKETMQ_STREAMS_STATE_CONSUMER_GROUP = "__state_group";
    public static final String COMMIT_STATE_INTERNAL_MS = "commitStateIntervalMillisecond";

    public static Integer STREAMS_PARALLEL_THREAD_NUM = 1;
    public static Integer SHUFFLE_TOPIC_QUEUE_NUM = 8;
    public static Integer SCHEDULED_THREAD_NUM = 2;
    public static Integer DEFAULT_ALLOW_LATE_MILLISECONDS = 2000;
}
