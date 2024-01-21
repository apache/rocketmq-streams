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
package org.apache.rocketmq.streams.window;

public class WindowConstants {

    public static final String SHUFFLE_OFFSET = "SHUFFLE_OFFSET";
    public static final String SPLIT_SIGN = "##";
    public static final String IS_COMPRESSION_MSG = "_is_compress_msg";
    public static final String COMPRESSION_MSG_DATA = "_compress_msg";
    public static final String MSG_FROM_SOURCE = "msg_from_source";
    public static final String ORIGIN_OFFSET = "origin_offset";
    public static final String ORIGIN_QUEUE_ID = "origin_queue_id";
    public static final String ORIGIN_QUEUE_IS_LONG = "origin_offset_is_LONG";
    public static final String ORIGIN_MESSAGE_HEADER = "origin_message_header";
    public static final String ORIGIN_SOURCE_NAME = "origin_offset_name";
    public static final String SHUFFLE_KEY = "SHUFFLE_KEY";
    public static final String ORIGIN_MESSAGE_TRACE_ID = "origin_request_id";
    public static final int DEFAULT_WINDOW_SIZE = 10;
    public static final int DEFAULT_WINDOW_SESSION_TIMEOUT = 10;
    public static final String IS_SYSTEME_MSG = "__is_system_msg";
    public static final String SYSTEME_MSG = "__system_msg";
    public static final String SYSTEME_MSG_CLASS = "__system_msg_class";
    protected static final String SHUFFLE_QUEUE_ID = "SHUFFLE_QUEUE_ID";
    protected static final String SHUFFLE_MESSAGES = "SHUFFLE_MESSAGES";
    private static final String SHUFFLE_TRACE_ID = "SHUFFLE_TRACE_ID";
    /**
     * 消息所属的window
     */
    protected String MSG_OWNER = "MSG_OWNER";
}
