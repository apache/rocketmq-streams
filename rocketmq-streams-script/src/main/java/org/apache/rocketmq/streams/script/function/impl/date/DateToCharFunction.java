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
package org.apache.rocketmq.streams.script.function.impl.date;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class DateToCharFunction {

    @FunctionMethod(value = "tochar", alias = "timestamp2string", comment = "把timestamp格式的数字转换成标准格式的字符串")
    public String toChar(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表timestamp格式的字段名或数字") String datetime) {
        return toChar(message, context, datetime, "yyyy-MM-dd HH:mm:ss");
    }

    @FunctionMethod(value = "linuxtochar", alias = "linuxtime2string", comment = "把linuxtime格式的数字转换成标准格式的字符串")
    public String linuxToChar(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表timestamp格式的字段名或数字") String datetime) {
        String result = null;
        String timeString = FunctionUtils.getValueString(message, context, datetime);
        if (timeString == null || "".equals(timeString)) {
            return result;
        }
        Long time = FunctionUtils.getLong(timeString) * 1000;
        Timestamp timestamp = new Timestamp(time);
        if (datetime == null) {
            return result;
        }
        Date date = new Date(timestamp.getTime());
        DateFormat dateFormat = DateUtil.getDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            result = dateFormat.format(date);
        } catch (Exception e) {
            throw new RuntimeException("tochar 执行错误", e);
        }
        return result;
    }

    /**
     * 将日期装换为字符串
     *
     * @param message
     * @param context
     * @param datetime
     * @param format
     * @return
     */
    @FunctionMethod(value = "tochar", alias = "timestamp2string", comment = "把timestamp格式的数字转换成指定格式的字符串")
    public String toChar(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表timestamp格式的字段名或数字") String datetime,
        @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        String result = null;
        String timeString = FunctionUtils.getValueString(message, context, datetime);
        Long time = FunctionUtils.getLong(timeString);
        format = FunctionUtils.getValueString(message, context, format);
        Timestamp timestamp = new Timestamp(time);
        if (datetime == null) {
            return result;
        }
        Date date = new Date(timestamp.getTime());
        DateFormat dateFormat = DateUtil.getDateFormat(format);
        try {
            result = dateFormat.format(date);
        } catch (Exception e) {
            throw new RuntimeException("tochar 执行错误", e);
        }
        return result;
    }
}
