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
public class LastDayFunction {

    /**
     * 获取本月最后一天日期
     *
     * @param message
     * @param context
     * @param date
     * @return
     */
    @FunctionMethod(value = "lastday", comment = "当前日期对应的月份中的最后一个天")
    public String lstDay(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表日期的字段名或常量") String date,
        @FunctionParamter(value = "string", comment = "代表时间格式的字段名或常量") String format) {
        Timestamp timestamp = null;
        date = FunctionUtils.getValueString(message, context, date);
        format = FunctionUtils.getValueString(message, context, format);
        if (date == null) {
            return null;
        }
        Date date1 = DateUtil.parse(date, format);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date1);
        calendar.set(Calendar.DAY_OF_MONTH, 1);
        calendar.add(Calendar.MONTH, 1);
        calendar.add(Calendar.DAY_OF_MONTH, -1);
        return DateUtil.format(calendar.getTime());
    }

    @FunctionMethod(value = "lastday", comment = "当前日期对应的月份中的最后一个天")
    public String lstDay(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表日期的字段名或常量") String date) {
        return lstDay(message, context, date, "'" + DateUtil.DEFAULT_FORMAT + "'");
    }

}
