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

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

import java.util.Date;

@Function
public class DateUtilFunction {

    @FunctionMethod(value = "isValidTime", alias = "isDate", comment = "当前字符串是否是日期")
    public Boolean isValidateDate(IMessage message, FunctionContext context,
                                  @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime) {
        return isValidateDate(message, context, datetime, "'" + DateUtil.DEFAULT_FORMAT + "'");
    }

    @FunctionMethod(value = "isValidTime", alias = "isDate", comment = "当前字符串是否是日期")
    public Boolean isValidateDate(IMessage message, FunctionContext context,
                                  @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                                  @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        datetime = FunctionUtils.getValueString(message, context, datetime);
        format = FunctionUtils.getValueString(message, context, format);
        return DateUtil.isValidTime(datetime, format);
    }

    @FunctionMethod(value = "addDay", alias = "dayAdd", comment = "给当前时间增加n天")
    public String addDay(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                         @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String months) {
        return addHour(message, context, datetime, "'" + DateUtil.DEFAULT_FORMAT + "'", months);
    }

    @FunctionMethod(value = "addDay", alias = "dayAdd", comment = "给当前时间增加n天")
    public String addDay(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                         @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format,
                         @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String months) {
        datetime = FunctionUtils.getValueString(message, context, datetime);
        format = FunctionUtils.getValueString(message, context, format);
        months = FunctionUtils.getValueString(message, context, months);
        Long value = FunctionUtils.getLong(months);
        Date date = DateUtil.parse(datetime, format);
        date = DateUtil.addDays(date, value.intValue());
        return DateUtil.format(date, format);
    }

    @FunctionMethod(value = "addMonth", alias = "monthAdd", comment = "给当前时间增加n月")
    public String addMonth(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                           @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String months) {
        return addHour(message, context, datetime, "'" + DateUtil.DEFAULT_FORMAT + "'", months);
    }

    @FunctionMethod(value = "addMonth", alias = "monthAdd", comment = "给当前时间增加n月")
    public String addMonth(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                           @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format,
                           @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String months) {
        datetime = FunctionUtils.getValueString(message, context, datetime);
        format = FunctionUtils.getValueString(message, context, format);
        months = FunctionUtils.getValueString(message, context, months);
        Long value = FunctionUtils.getLong(months);
        Date date = DateUtil.parse(datetime, format);
        date = DateUtil.addMonths(date, value.intValue());
        return DateUtil.format(date, format);
    }

    @FunctionMethod(value = "addYear", alias = "yearAdd", comment = "给当前时间增加n年")
    public String addYear(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                          @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String years) {
        return addHour(message, context, datetime, "'" + DateUtil.DEFAULT_FORMAT + "'", years);
    }

    @FunctionMethod(value = "addYear", alias = "yearAdd", comment = "给当前时间增加n年")
    public String addYear(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                          @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format,
                          @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String years) {
        datetime = FunctionUtils.getValueString(message, context, datetime);
        format = FunctionUtils.getValueString(message, context, format);
        years = FunctionUtils.getValueString(message, context, years);
        Long value = FunctionUtils.getLong(years);
        Date date = DateUtil.parse(datetime, format);
        date = DateUtil.addYears(date, value.intValue());
        return DateUtil.format(date, format);
    }

    @FunctionMethod(value = "addHour", alias = "hourAdd", comment = "给当前时间增加n个小时")
    public String addHour(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                          @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String hours) {
        return addHour(message, context, datetime, "'" + DateUtil.DEFAULT_FORMAT + "'", hours);
    }

    @FunctionMethod(value = "addHour", alias = "hourAdd", comment = "给当前时间增加n个小时")
    public String addHour(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                          @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format,
                          @FunctionParamter(value = "string", comment = "代表待增加时间的字段名，数字或常量") String hours) {
        datetime = FunctionUtils.getValueString(message, context, datetime);
        format = FunctionUtils.getValueString(message, context, format);
        hours = FunctionUtils.getValueString(message, context, hours);
        Long hourValue = FunctionUtils.getLong(hours);
        Date date = DateUtil.parse(datetime, format);
        date = DateUtil.addHour(date, hourValue.intValue());
        return DateUtil.format(date, format);
    }
}
