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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class DateAddFunction {

    /**
     * @param message
     * @param context
     * @param dateTime
     * @param delta
     * @param datepart
     * @return 日期加操作
     */

    @FunctionMethod(value = "timeadd", alias = "timeadd", comment = "对指定的时间进行年月日时分秒单位的增加，负数代表减少")
    public String timestampadd(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "string", comment = "代表标准格式日期的字段名或常量") String dateTime,
                               @FunctionParamter(value = "string", comment = "代表要增加时间单位的字段名或常量，包括YEAR，MONTH,DAY,HOUR,MINUTE,WEEK,SECOND,"
                                   + "FRAC_SECOND") String datepart,
                               @FunctionParamter(value = "string", comment = "增加的数值，可以是数字，常量和代表数值的字段名，支持负值") String delta) {
        String dateTimeStr = FunctionUtils.getValueString(message, context, dateTime);
        String deltaStr = FunctionUtils.getValueString(message, context, delta);
        String datepartStr = FunctionUtils.getValueString(message, context, datepart);
        if (dateTime == null || datepart == null) {
            return null;
        }
        int deltai = Integer.parseInt(deltaStr);
        Date date = DateUtil.parse(dateTimeStr);
        datepartStr = datepartStr.toUpperCase();
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if ("YEAR".equals(datepartStr)) {
            cal.add(Calendar.YEAR, deltai);
        }
        if ("MONTH".equals(datepartStr)) {
            cal.add(Calendar.MONTH, deltai);
        }
        if ("DAY".equals(datepartStr)) {
            cal.add(Calendar.DATE, deltai);
        }
        if ("HOUR".equals(datepartStr)) {
            cal.add(Calendar.HOUR, deltai);
        }
        if ("MINUTE".equals(datepartStr)) {
            cal.add(Calendar.MINUTE, deltai);
        }
        if ("WEEK".equals(datepartStr)) {
            cal.add(Calendar.WEEK_OF_YEAR, deltai);
        }
        if ("SECOND".equals(datepartStr)) {
            cal.add(Calendar.SECOND, deltai);
        }
        if ("FRAC_SECOND".equals(datepartStr)) {
            cal.add(Calendar.MILLISECOND, deltai);
        }
        return DateUtil.format(cal.getTime());
    }

    /**
     * 返回日期或日期时间表达式datetime_expr加上int_expr值的时间结果
     *
     * @param message
     * @param context
     * @param dateTime
     * @param delta
     * @param datepart
     * @return 日期加操作
     */

    @FunctionMethod(value = "dateadd", alias = "adddate", comment = "对指定的时间进行年月日时分秒单位的增加，负数代表减少")
    public String dateadd(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表标准格式日期的字段名或常量") String dateTime,
                          @FunctionParamter(value = "string", comment = "代表要增加时间单位的字段名或常量，包括YEAR，MONTH,DAY,HOUR,MINUTE,WEEK,SECOND,"
                              + "FRAC_SECOND") String datepart,
                          @FunctionParamter(value = "string", comment = "增加的数值，可以是数字，常量和代表数值的字段名，支持负值") String delta) {
        Timestamp timestamp = null;
        String dateTimeStr = FunctionUtils.getValueString(message, context, dateTime);
        String deltaStr = FunctionUtils.getValueString(message, context, delta);
        String datepartStr = FunctionUtils.getValueString(message, context, datepart);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if (dateTime == null || datepart == null) {
            return null;
        }
        int deltai = Integer.parseInt(deltaStr);
        Date date = null;
        try {
            date = dateFormat.parse(dateTimeStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if (datepartStr.equals("yyyy") || "year".equals(datepartStr)) {
            cal.add(Calendar.YEAR, deltai);
        }
        if (datepartStr.equals("MM") || "month".equals(datepartStr)) {
            cal.add(Calendar.MONTH, deltai);
        }
        if (datepartStr.equals("dd") || "day".equals(datepartStr)) {
            cal.add(Calendar.DATE, deltai);
        }
        if (datepartStr.equals("hh") || "hour".equals(datepartStr)) {
            cal.add(Calendar.HOUR, deltai);
        }
        if (datepartStr.equals("mm") || "minute".equals(datepartStr)) {
            cal.add(Calendar.MINUTE, deltai);
        }
        if (datepartStr.equals("ss") || "second".equals(datepartStr)) {
            cal.add(Calendar.SECOND, deltai);
        }
        return dateFormat.format(cal.getTime());
    }

}
