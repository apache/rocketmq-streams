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

public class BatchFinishMessage implements ISystemMessage {
    public static String PRIMARY_KEY = "__Primary_KEY";
    protected IMessage message;

    public BatchFinishMessage(IMessage message) {
        this.message = message;
    }

    public static IMessage create() {
        JSONObject msg = new JSONObject();
        msg.put(PRIMARY_KEY, true);
        return new Message(msg);
    }

    public static boolean isBatchFinishMessage(IMessage message) {
        try {
            return message.getMessageBody().getBooleanValue(PRIMARY_KEY);
        } catch (Throwable t) {
            return false;
        }
    }

    public IMessage getMsg() {
        return message;
    }
}
