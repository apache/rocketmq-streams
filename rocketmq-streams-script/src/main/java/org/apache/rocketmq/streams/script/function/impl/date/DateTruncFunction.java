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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class DateTruncFunction {

    @FunctionMethod(value = "for_time", alias = "timeInterval", comment = "根据时间段来进行数据拆分")
    public Integer splitByTimeInterval(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表时间的字段名称或常量") String from,
        @FunctionParamter(value = "string", comment = "代表时间的字段名称或常量") String until,
        @FunctionParamter(value = "string", comment = "代表要增加时间单位的字段名或常量，包括YEAR，MONTH,DAY,HOUR,MINUTE,WEEK,SECOND,"
            + "FRAC_SECOND") String datepart,
        @FunctionParamter(value = "string", comment = "代表年月日的字段名或常量，支持year，day，month") String delta) {
        String fromValue = FunctionUtils.getValueString(message, context, from);
        String untilValue = FunctionUtils.getValueString(message, context, until);
        String datepartValue = FunctionUtils.getValueString(message, context, datepart);
        String deltaValue = FunctionUtils.getValueString(message, context, delta);
        Integer gap = Integer.valueOf(deltaValue);
        JSONObject msg = new JSONObject();
        msg.putAll(message.getMessageBody());
        List<IMessage> messages = new ArrayList<>();
        while (fromValue.compareTo(untilValue) < 0) {
            msg.put(from, fromValue);
            messages.add(new Message(msg));
            fromValue = (String) context.executeFunction("adddate", message, context, "'" + fromValue + "'",
                "'" + datepartValue + "'", "'" + deltaValue + "'");
            msg = new JSONObject();
            msg.putAll(message.getMessageBody());

        }
        context.openSplitModel();
        context.setSplitMessages(messages);
        return messages.size();
    }

    @FunctionMethod(value = "datetrunc", alias = "datefirst", comment = "获取标准时间格式的年月日的起始时间")
    public String dateTrunc(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表时间的字段名称或常量") String datetime,
        @FunctionParamter(value = "string", comment = "代表年月日的字段名或常量，支持year，day，month") String datepart) {
        return dateTrunc(message, context, datetime, datepart, "'" + DateUtil.DEFAULT_FORMAT + "'");
    }

    /**
     * 截取指定日期转化为新的日期
     *
     * @param message
     * @param context
     * @param datetime
     * @param datepart
     * @return
     */
    @FunctionMethod(value = "datetrunc", alias = "datefirst", comment = "获取标准时间格式的年月日的起始时间")
    public String dateTrunc(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表时间的字段名称或常量") String datetime,
        @FunctionParamter(value = "string", comment = "代表年月日的字段名或常量，支持year，day，month") String datepart,
        @FunctionParamter(value = "string", comment = "代表时间格式的字段名或常量") String format) {
        if (datetime == null || datepart == null) {
            return null;
        }
        datetime = FunctionUtils.getValueString(message, context, datetime);
        datepart = FunctionUtils.getValueString(message, context, datepart).toLowerCase();
        format = FunctionUtils.getValueString(message, context, format);
        if (StringUtil.isEmpty(format)) {
            format = DateUtil.DEFAULT_FORMAT;
        }
        Date date1 = DateUtil.parse(datetime, format);

        String result = "";
        if (datepart.equals("yyyy") || "year".equals(datepart)) {
            result = DateUtil.getYear(date1) + "-01-01 00:00:00";
        }
        if (datepart.equals("mm") || "month".equals(datepart)) {
            String year = DateUtil.getYear(date1);
            int month = DateUtil.getMonth(date1);
            String monthStr = month < 10 ? "0" + month : month + "";
            result = year + "-" + monthStr + "-01 00:00:00";
        }
        if (datepart.equals("dd") || "day".equals(datepart)) {
            String year = DateUtil.getYear(date1);
            int month = DateUtil.getMonth(date1);
            String monthStr = month < 10 ? "0" + month : month + "";
            int dayValue = DateUtil.getDay(date1);
            String dayStr = dayValue < 10 ? "0" + dayValue : dayValue + "";
            result = year + "-" + monthStr + "-" + dayStr + " 00:00:00";
        }
        if (datepart.equals("hh") || "hour".equals(datepart)) {
            String year = DateUtil.getYear(date1);
            int month = DateUtil.getMonth(date1);
            String monthStr = month < 10 ? "0" + month : month + "";
            int dayValue = DateUtil.getDay(date1);
            String dayStr = dayValue < 10 ? "0" + dayValue : dayValue + "";
            int hourValue = DateUtil.getHour(date1);
            String hourStr = hourValue < 10 ? "0" + hourValue : hourValue + "";
            result = year + "-" + monthStr + "-" + dayStr + " " + hourStr + ":00:00";
        }
        if (datepart.equals("mm") || "minute".equals(datepart)) {
            String year = DateUtil.getYear(date1);
            int month = DateUtil.getMonth(date1);
            String monthStr = month < 10 ? "0" + month : month + "";
            int dayValue = DateUtil.getDay(date1);
            String dayStr = dayValue < 10 ? "0" + dayValue : dayValue + "";
            int hourValue = DateUtil.getHour(date1);
            String hourStr = hourValue < 10 ? "0" + hourValue : hourValue + "";
            int minuteValue = DateUtil.getMinute(date1);
            String minuteStr = minuteValue < 10 ? "0" + minuteValue : minuteValue + "";
            result = year + "-" + monthStr + "-" + dayStr + " " + hourStr + ":" + minuteStr + ":00";
        }
        return result;
    }
}
