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
package org.apache.rocketmq.streams.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IJsonobjectProcessor;

public class MessageUtil {

    private static final Log LOG = LogFactory.getLog(MessageUtil.class);

    public static JSONObject reprocessingSingleMessage(String data, boolean isJsonData, boolean isArrayData,
                                                       IJsonobjectProcessor messageProcessor) {
        JSONArray jsonArray = reprocessingMessage(data, isJsonData, isArrayData, messageProcessor);
        if (jsonArray == null || jsonArray.size() == 0) {
            return null;
        }
        if (jsonArray.size() == 1) {
            return jsonArray.getJSONObject(0);
        }
        throw new RuntimeException("expect one result ,actual is " + jsonArray.size());
    }

    public static JSONArray reprocessingMessage(String data, boolean isJsonData, boolean isArrayData,
                                                IJsonobjectProcessor messageProcessor) {
        try {
            return reprocessingMessage0(data, isJsonData, isArrayData, messageProcessor);
        } catch (Exception e) {
            if (LOG.isErrorEnabled()) {
                LOG.error("reprocessingMessage error" + data, e);
            }
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static JSONArray reprocessingMessage0(String data, boolean isJsonData, boolean isArrayData,
                                                 IJsonobjectProcessor messageProcessor) {
        JSONObject jsonObject = null;
        JSONArray jsonArray = new JSONArray();
        if (isArrayData) {
            jsonArray = JSONObject.parseArray(data);
        } else {
            if (!isJsonData) {
                jsonObject = new JSONObject();
                jsonObject.put(IMessage.DATA_KEY, data);
                jsonObject.put(IMessage.IS_NOT_JSON_MESSAGE, true);
            } else {
                jsonObject = JSON.parseObject(data);
            }
            jsonArray.add(jsonObject);
        }

        JSONArray array = new JSONArray();
        if (CollectionUtil.isNotEmpty(jsonArray)) {
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject msgBody = jsonArray.getJSONObject(i);
                AbstractContext<IMessage> context = messageProcessor.doMessage(msgBody);
                if (!context.isSplitModel()) {
                    array.add(context.getMessage().getMessageBody());
                } else {
                    List<IMessage> messages = context.getSplitMessages();
                    for (IMessage message : messages) {
                        array.add(message.getMessageBody());
                    }
                }
            }
        }
        return array;
    }

    public static JSONArray reprocessingMessage(JSONObject data, IJsonobjectProcessor messageProcessor) {
        return reprocessingMessage(data.toJSONString(), true, false, messageProcessor);
    }

    public static JSONArray reprocessingMessage(JSONArray data, IJsonobjectProcessor messageProcessor) {
        return reprocessingMessage(data.toJSONString(), true, true, messageProcessor);
    }

}
