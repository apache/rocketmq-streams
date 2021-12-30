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
import java.util.UUID;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public class Message implements IMessage {

    private JSONObject message;

    private boolean isJsonMessage = true;

    protected ISystemMessage systemMessage;

    protected MessageHeader header = new MessageHeader();

    public Message(JSONObject message) {
        this.message = message;
        if (!(message instanceof UserDefinedMessage)) {
            String messageValue = message.getString(UserDefinedMessage.class.getName());
            if (messageValue != null) {
                this.message = new UserDefinedMessage(message, messageValue);
            }
        }

        if (message.containsKey(TraceUtil.TRACE_ID_FLAG)) {
            this.header.setTraceId(message.getString(TraceUtil.TRACE_ID_FLAG));
        } else {
            this.header.setTraceId(UUID.randomUUID().toString());
        }
    }

    @Override
    public MessageHeader getHeader() {
        return this.header;
    }

    @Override
    public JSONObject getMessageBody() {
        return this.message;
    }

    @Override
    public void setMessageBody(JSONObject messageBody) {
        this.message = messageBody;
    }

    @Override
    public boolean isJsonMessage() {
        return isJsonMessage;
    }

    @Override
    public Object getMessageValue() {
        if (this.message instanceof UserDefinedMessage) {
            UserDefinedMessage userDefinedMessage = (UserDefinedMessage) this.message;
            return userDefinedMessage.getMessageValue();
        }
        return this.message;
    }

    @Override
    public Message copy() {
        Message msg = new Message(this.message);
        msg.header = getHeader().copy();
        return msg;
    }

    @Override
    public Message deepCopy() {
        JSONObject jsonObject = new JSONObject();
        if (this.message instanceof UserDefinedMessage) {
            jsonObject = new UserDefinedMessage(((UserDefinedMessage) this.message).getMessageValue());
        }

        for (String key : message.keySet()) {
            jsonObject.put(key, message.get(key));
        }
        Message message = new Message(jsonObject);
        message.setSystemMessage(getSystemMessage());
        message.isJsonMessage = isJsonMessage;
        message.header = getHeader().copy();
        return message;
    }

    public static JSONObject parseObject(String msg) {
        JSONObject jsonObject = JSONObject.parseObject(msg);
        if (jsonObject != null) {
            String userObjectString = jsonObject.getString(UserDefinedMessage.class.getName());
            if (userObjectString != null) {
                return new UserDefinedMessage(jsonObject, userObjectString);
            }
        }
        return jsonObject;
    }

    public void setJsonMessage(boolean jsonMessage) {
        isJsonMessage = jsonMessage;
    }

    @Override
    public void setHeader(MessageHeader header) {
        this.header = header;
    }

    @Override
    public ISystemMessage getSystemMessage() {
        return systemMessage;
    }

    public void setSystemMessage(ISystemMessage systemMessage) {
        this.systemMessage = systemMessage;
    }

}
