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
package org.apache.rocketmq.streams.common.batchsystem;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.topology.model.MessageFinishCallBack;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 系统消息，当确定后续数据了，可以发这个消息，这个消息会告诉所有组件，完成flush和计算，会在整个拓扑传递
 * 主要在于测试场景，测试一小批次数据，或批处理场景
 */
public class BatchFinishMessage implements ISystemMessage {
    public static String PRIMARY_KEY = "__Primary_KEY";
    public static String CALLBACK = "__callback";
    public static String MESSAGE_ID = "__message_id";
    protected IMessage message;

    public BatchFinishMessage(IMessage message) {
        this.message = message;

    }

    public static IMessage create(MessageFinishCallBack messageFinishCallBack) {
        JSONObject msg = new JSONObject();
        msg.put(PRIMARY_KEY, true);
        msg.put(CALLBACK, messageFinishCallBack);
        msg.put(MESSAGE_ID, StringUtil.getUUID());
        return new Message(msg);
    }

    public static IMessage create() {
        JSONObject msg = new JSONObject();
        msg.put(PRIMARY_KEY, true);
        msg.put(MESSAGE_ID, StringUtil.getUUID());
        return new Message(msg);
    }

    public static boolean isBatchFinishMessage(IMessage message) {
        if (message.getMessageValue() instanceof JSONObject) {
            return message.getMessageBody().getBooleanValue(PRIMARY_KEY);
        } else {
            return false;
        }
    }

    public MessageFinishCallBack getMessageFinishCallBack(IMessage msg) {
        return (MessageFinishCallBack) msg.getMessageBody().get(CALLBACK);
    }

    public String getMessageId() {
        return this.message.getMessageBody().getString(MESSAGE_ID);
    }

    public IMessage getMsg() {
        return message;
    }

    @Override public int getSystemMessageType() {
        return ISystemMessage.BATCH_FINISH;
    }
}
