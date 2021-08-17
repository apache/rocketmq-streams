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
package org.apache.rocketmq.streams.common.context;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;

public interface IMessage {
    /**
     * 对于非json数据在处理前先转化成json结构，key＝data；value＝实际数据
     */
    public static final String DATA_KEY = "data";

    String SHUFFLE_MESSAGE_FLAG = "_shuffle_msg";//是否是shuffle msg

    /**
     * 是否为json格式
     */
    public static final String IS_NOT_JSON_MESSAGE = "isNotJsonMessage";

    /**
     * used when trace id lost
     */
    public static final String DEFAULT_MESSAGE_TRACE_ID = "00000000-0000-0000-0000-000000000000";

    MessageHeader getHeader();

    void setHeader(MessageHeader header);

    /**
     * 获取具体消息，目前只支持json结构
     *
     * @return json
     */
    JSONObject getMessageBody();

    /**
     * 设置json格式的消息
     *
     * @param messageBody json
     */
    void setMessageBody(JSONObject messageBody);

    /**
     * 是否是json格式
     *
     * @return 是否json
     */
    boolean isJsonMessage();

    <T extends IMessage> T copy();

    <T extends IMessage> T deepCopy();

    /**
     * 获取message 对象，如果是用户自定义类型，返回具体值，否则返回json
     *
     * @return message
     */
    Object getMessageValue();

    /**
     * 如果是系统消息，获取checkpoint消息
     *
     * @return
     */
    ISystemMessage getSystemMessage();

}
