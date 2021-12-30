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
package org.apache.rocketmq.streams.script.function.impl.flatmap;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class SplitArrayFunction {
    private static final Log LOG = LogFactory.getLog(SplitArrayFunction.class);
    public static final String SPLIT_LAST_FLAG = "__split_last_flag";//对于拆分的消息，会在最后一条加一个标识别。
    public static final String FUNTION_NAME = "splitArray";

    /**
     * 把一个json结构中包含的数组平铺起来
     *
     * @param channelMessage
     * @param context
     * @param splitFieldName
     */
    @FunctionMethod(value = FUNTION_NAME, alias = "split_array", comment = "拆分json数组")
    public void splitArray(IMessage channelMessage, FunctionContext context,
        @FunctionParamter(value = "string", comment = "值为数组的字段名") String splitFieldName) {
        boolean needFlush = channelMessage.getHeader().isNeedFlush();
        context.openSplitModel();
        if (StringUtil.isEmpty(splitFieldName)) {
            return;
        }
        String tmp = FunctionUtils.getValueString(channelMessage, context, splitFieldName);

        Object object = channelMessage.getMessageBody().get(tmp);
        if (object == null) {
            object = channelMessage.getMessageBody().get(splitFieldName);
        }
        if (object == null) {
            return;
        }
        List jsonArray = null;

        if (JSONArray.class.isInstance(object) || List.class.isInstance(object)) {
            jsonArray = (List) object;
        }
        if (String.class.isInstance(object)) {
            jsonArray = JSONArray.parseArray((String) object);
        }

        if (CollectionUtil.isEmpty(jsonArray)) {
            return;
        }

        for (int i = 0; i < jsonArray.size(); i++) {
            Object value = jsonArray.get(i);
            IMessage newMessage = null;
            if (IMessage.class.isInstance(value)) {
                IMessage message = (IMessage) value;
                //如0果这条数据需要刷新，则只需要最后一条刷新即可

                message.getMessageBody().putAll(channelMessage.getMessageBody());
                newMessage = message;
            } else if (Map.class.isInstance(value)) {
                JSONObject jsonObject = copyJsonObjectExceptField(channelMessage.getMessageBody(), splitFieldName);
                Map subJsonObject = (Map) value;
                jsonObject.putAll(subJsonObject);
                IMessage copyMessage = channelMessage.copy();
                copyMessage.setMessageBody(jsonObject);
                newMessage = copyMessage;
            } else {
                LOG.warn("can not support split item , the value is " + value.getClass().getName());
                continue;
            }
            if (i < jsonArray.size() - 1) {
                newMessage.getHeader().setNeedFlush(false);
            } else {
                newMessage.getHeader().setNeedFlush(needFlush);
            }
            context.removeSpliteMessage(channelMessage);
            context.addSplitMessages(newMessage);
        }

    }

    /**
     * 把一个json结构中包含的数组平铺起来
     *
     * @param channelMessage
     * @param context
     * @param splitFieldName
     */
    @FunctionMethod(value = "split", alias = "STRING_SPLIT", comment = "字符串按分割符分割")
    public void splitA(IMessage channelMessage, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字符串或字段名字") String splitFieldName, String sign) {
        if (StringUtil.isEmpty(splitFieldName)) {
            return;
        }
        String tmp = FunctionUtils.getValueString(channelMessage, context, splitFieldName);
        sign = FunctionUtils.getValueString(channelMessage, context, sign);
        if (tmp == null || sign == null) {
            return;
        }
        String[] values = tmp.split(sign);
        if (values == null || values.length == 0) {
            return;
        }
        boolean needFlush = channelMessage.getHeader().isNeedFlush();
        context.openSplitModel();
        int index = 0;
        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            if ("null".equalsIgnoreCase(value)) {
                continue;
            }
            if (StringUtil.isEmpty(value)) {
                continue;
            }
            IMessage newMessage = channelMessage.deepCopy();
            newMessage.getMessageBody().put(FunctionType.UDTF.getName() + index, value);
            index++;
            newMessage.getHeader().setTraceId(channelMessage.getHeader().getTraceId() + "_" + i);
            if (i < values.length - 1) {
                newMessage.getHeader().setNeedFlush(false);
            } else {
                newMessage.getHeader().setNeedFlush(needFlush);
            }
            context.removeSpliteMessage(channelMessage);
            context.addSplitMessages(newMessage);
        }

    }

    /**
     * 把json进行复制，并忽略数组字段
     *
     * @param messageBody
     * @param splitFieldName
     * @return
     */
    private JSONObject copyJsonObjectExceptField(JSONObject messageBody, String splitFieldName) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.putAll(messageBody);
        jsonObject.remove(splitFieldName);
        return jsonObject;
    }

}
