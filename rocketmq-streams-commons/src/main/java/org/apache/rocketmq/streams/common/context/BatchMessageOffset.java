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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class BatchMessageOffset extends BasedConfigurable {

    public static final String TYPE = "BatchMessageOffset";

    protected String currentMessageKey = "0";//当前处理的消息，在map中key
    protected String ownerType;

    protected Map<String, String> messages = new HashMap<>();//如果需要存储多组offset，可以用map的结构

    /**
     * 最后更新时间
     */
    protected long lastUpdateTime = System.currentTimeMillis();

    public BatchMessageOffset() {
        setType(TYPE);
    }

    public static BatchMessageOffset create(IConfigurable configurable, String msg, String offsetName, String currentMessageKey) {
        BatchMessageOffset offset = new BatchMessageOffset();
        offset.setNameSpace(configurable.getNameSpace());
        offset.setConfigureName(offsetName);
        offset.ownerType = configurable.getType();
        if (StringUtil.isEmpty(msg)) {
            offset.getMessages().put(currentMessageKey, new JSONObject().toJSONString());
        } else {
            offset.getMessages().put(currentMessageKey, msg);
        }

        return offset;
    }

    public JSONObject getCurrentMsg() {
        String msgStr = getMessages().get(currentMessageKey);
        if (msgStr == null) {
            return null;
        }
        msgStr = msgStr.replace("”", "\"");
        return JSON.parseObject(msgStr);
    }

    public JSONObject getMsg(int i) {

        String msgStr = messages.get(i);
        if (msgStr == null) {
            return null;
        }
        msgStr = msgStr.replace("”", "\"");
        return JSON.parseObject(msgStr);
    }

    public String getMessage(int i) {
        return messages.get(i);
    }

    public String getCurrentMessage() {
        return getMessages().get(currentMessageKey);
    }

    public void setCurrentMessage(String message) {
        this.messages.put(currentMessageKey, message);
        lastUpdateTime = System.currentTimeMillis();
    }

    public Map<String, String> getMessages() {
        return messages;
    }

    public String getOwnerType() {
        return ownerType;
    }

    public void setOwnerType(String ownerType) {
        this.ownerType = ownerType;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public String getCurrentMessageKey() {
        return currentMessageKey;
    }

    public void setCurrentMessageKey(String currentMessageKey) {
        this.currentMessageKey = currentMessageKey;
    }

    public void setMessages(Map<String, String> messages) {
        this.messages = messages;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }
}
