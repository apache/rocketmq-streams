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

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class AddMonthFunction {

    /**
     * 给指定的日期加指定月数
     *
     * @param message
     * @param context
     * @param datetime
     * @param nummonths
     * @return
     */
    @FunctionMethod(value = "addmonth", comment = "增加指定日期的月数")
    public String addmonth(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表日期的字段名或常量") String datetime,
        @FunctionParamter(value = "string", comment = "代表日期格式的字段名或常量") String formatName,
        @FunctionParamter(value = "string", comment = "待增加的月数，可以是数字，常量和代表月数的字段名") String nummonths) {
        String result = null;
        String format = FunctionUtils.getValueString(message, context, formatName);
        datetime = FunctionUtils.getValueString(message, context, datetime);
        int nummonthsi = FunctionUtils.getLong(nummonths).intValue();
        if (datetime == null) {
            return result;
        }
        DateFormat dateFormat = DateUtil.getDateFormat(format);
        Date dateSource = null;
        try {
            dateSource = dateFormat.parse(datetime);
        } catch (ParseException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        Date targetDate = DateUtil.addMonths(dateSource, nummonthsi);
        result = dateFormat.format(targetDate);
        return result;
    }

    @FunctionMethod(value = "addmonth", comment = "增加指定日期的月数")
    public String addmonth(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表日期的字段名或常量") String datetime,
        @FunctionParamter(value = "string", comment = "代表日期格式的字段名或常量") String nummonths) {
        return addmonth(message, context, datetime, "yyyy-MM-dd HH:mm:ss", nummonths);
    }
}
