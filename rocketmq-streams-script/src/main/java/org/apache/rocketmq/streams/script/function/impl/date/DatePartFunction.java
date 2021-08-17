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

import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class DatePartFunction {

    @FunctionMethod(value = "datepart", alias = "date", comment = "获取标准格式日期的年月日时分秒")
    public String datePart(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                           @FunctionParamter(value = "string", comment = "代表年月日时分秒的字段名或常量，值为year,month,day,hour,minute,second")
                               String datepart) {
        return datePart(message, context, datetime, ("'" + DateUtil.DEFAULT_FORMAT + "'"), datepart);
    }

    /**
     * 获取指定的日期
     *
     * @param message
     * @param context
     * @param datetime
     * @param datepart
     * @return
     */
    @FunctionMethod(value = "datepart", alias = "date", comment = "获取标准格式日期的年月日时分秒")
    public String datePart(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                           @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format,
                           @FunctionParamter(value = "string", comment = "代表年月日时分秒的字段名或常量，值为year,month,day,hour,minute,second")
                               String datepart) {
        String result = null;
        if (datetime == null || datepart == null) {
            return result;
        }
        datetime = FunctionUtils.getValueString(message, context, datetime);
        datepart = FunctionUtils.getValueString(message, context, datepart);
        format = FunctionUtils.getValueString(message, context, format);
        Date date1 = DateUtil.parse(datetime, format);
        if (datepart.equals("yyyy") || "year".equals(datepart)) {
            return DateUtil.getYear(date1) + "";
        }
        if (datepart.equals("MM") || "month".equals(datepart)) {
            return DateUtil.getMonth(date1) + "";
        }
        if (datepart.equals("dd") || "day".equals(datepart)) {
            return DateUtil.getDay(date1) + "";
        }
        if (datepart.equals("hh") || "hour".equals(datepart)) {
            return DateUtil.getHour(date1) + "";
        }
        if (datepart.equals("mm") || "minute".equals(datepart)) {
            return DateUtil.getMinute(date1) + "";
        }
        if (datepart.equals("ss") || "second".equals(datepart)) {
            return DateUtil.getSecond(date1) + "";
        }
        return result;
    }

    @FunctionMethod(value = "day", alias = "dd", comment = "获取日期中的日")
    public Integer day(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime,
                       @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        String value = datePart(message, context, datetime, format, "'day'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "day", alias = "dd", comment = "获取日期中的日")
    public Integer day(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime) {
        String value = datePart(message, context, datetime, "'day'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "day", alias = "dd", comment = "获取日期中的日")
    public Integer day(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "long", comment = "代表标准时间格式的字段名或常量") Long datetime) {
        Date date = new Date(datetime);
        return DateUtil.getDay(date);
    }

    @FunctionMethod(value = "hour", alias = "hh", comment = "获取日期中的小时")
    public Integer hour(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime,
                        @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        String value = datePart(message, context, datetime, format, "'hour'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "hour", alias = "hh", comment = "获取日期中的小时")
    public Integer hour(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime) {
        String value = datePart(message, context, datetime, "'hour'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "year", alias = "yyyy", comment = "获取日期中的年")
    public Integer year(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime,
                        @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        String value = datePart(message, context, datetime, format, "'year'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "year", alias = "yyyy", comment = "获取日期中的年")
    public Integer year(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime) {
        String value = datePart(message, context, datetime, "'year'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "month", comment = "获取日期中的月")
    public Integer month(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime,
                         @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        String value = datePart(message, context, datetime, format, "'month'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "month", comment = "获取日期中的月")
    public Integer month(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime) {
        String value = datePart(message, context, datetime, "'month'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "minute", comment = "获取日期中的分钟")
    public Integer minute(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime,
                          @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        String value = datePart(message, context, datetime, format, "'minute'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "minute", comment = "获取日期中的分钟")
    public Integer minute(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime) {
        String value = datePart(message, context, datetime, "'minute'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "second", alias = "ss", comment = "获取日期中的秒")
    public Integer second(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime,
                          @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        String value = datePart(message, context, datetime, format, "'second'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }

    @FunctionMethod(value = "second", alias = "ss", comment = "获取日期中的秒")
    public Integer second(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表标准时间格式的字段名或常量") String datetime) {
        String value = datePart(message, context, datetime, "'second'");
        if (value == null) {
            return null;
        }
        return Integer.valueOf(value);
    }
}
