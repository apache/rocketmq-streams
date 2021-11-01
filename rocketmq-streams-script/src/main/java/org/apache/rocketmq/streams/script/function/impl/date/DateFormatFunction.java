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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class DateFormatFunction {

    /**
     * @param message
     * @param context
     * @param dateFieldName
     */
    @FunctionMethod(value = "format", comment = "把指定的标准时间转换成某种特定的格式")
    public String format(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String dateFieldName,
                         @FunctionParamter(value = "string", comment = "代表转换时间格式的字段名或常量") String destformat) {
        String value = FunctionUtils.getValueString(message, context, dateFieldName);
        destformat = FunctionUtils.getValueString(message, context, destformat);
        if(FunctionUtils.isLong(value)){
            try {
                SimpleDateFormat destDateFormat = new SimpleDateFormat(destformat);
                Date date=new Date(Long.valueOf(value));
                String dateStr = destDateFormat.format(date);
                return dateStr;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("format函数执行错误", e);
            }

        }else {
            return format(message,context,dateFieldName,"'yyyy-MM-dd HH:mm:ss'",destformat);
        }
    }

    /**
     * @param message
     * @param context
     * @param dateFieldName
     */
    @FunctionMethod(value = "format", comment = "把指定的时间转换成某种特定的格式")
    public String format(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String dateFieldName,
                         @FunctionParamter(value = "string", comment = "代表现有时间格式的字段名或常量") String oriformat,
                         @FunctionParamter(value = "string", comment = "代表转换时间格式的字段名或常量") String destformat) {
        String value = FunctionUtils.getValueString(message, context, dateFieldName);
        oriformat = FunctionUtils.getValueString(message, context, oriformat);
        destformat = FunctionUtils.getValueString(message, context, destformat);
        SimpleDateFormat oriDateFormat = new SimpleDateFormat(oriformat);
        SimpleDateFormat destDateFormat = new SimpleDateFormat(destformat);
        try {
            Date date = oriDateFormat.parse(value);
            String dateStr = destDateFormat.format(date);
            return dateStr;
        } catch (ParseException e) {
            e.printStackTrace();
            throw new RuntimeException("format函数执行错误", e);
        }

    }

    @Deprecated
    @FunctionMethod(value = "dateFormat", alias = "modifyFormat", comment = "修改标准时间字段为某种时间格式")
    public String dateFormat(IMessage message, FunctionContext context,
                             @FunctionParamter(value = "string", comment = "代表时间的字段名,不支持常量") String dateFieldName,
                             @FunctionParamter(value = "string", comment = "默认为格式常量，不加引号") String format) {
        String value = FunctionUtils.getValueString(message, context, dateFieldName);
        if (value == null) {
            return "";
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SimpleDateFormat destFormat = new SimpleDateFormat(format);
        try {
            Date date = dateFormat.parse(value);
            String dateStr = dateFormat.format(date);
            message.getMessageBody().put(dateFieldName, dateStr);
            return dateStr;
        } catch (ParseException e) {
            e.printStackTrace();
            return null;
        }
    }
}
