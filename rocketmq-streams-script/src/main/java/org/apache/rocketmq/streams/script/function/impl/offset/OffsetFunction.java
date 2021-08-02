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
package org.apache.rocketmq.streams.script.function.impl.offset;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.BatchMessageOffset;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class OffsetFunction {

    /**
     * 抽取部分字段做进度保存
     *
     * @param message
     * @param context
     * @param fieldNames
     */
    @FunctionMethod(value = "offset", alias = "progress", comment = "设置offset")
    public void extractOffset(IMessage message, AbstractContext context,
                              @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String... fieldNames) {
        if (fieldNames == null) {
            return;
        }
        MessageHeader messageHeader = message.getHeader();
        BatchMessageOffset offset = messageHeader.getProgress();
        ;
        JSONObject msg = null;
        if (offset == null) {
            offset = new BatchMessageOffset();
            msg = new JSONObject();
        } else {
            msg = offset.getCurrentMsg();
        }

        for (String fieldName : fieldNames) {
            String value = FunctionUtils.getValueString(message, context, fieldName);
            if (value != null) {
                msg.put(fieldName, value);
            }
        }
        offset.setCurrentMessage(msg.toJSONString());
        messageHeader.setProgress(offset);
    }

}
