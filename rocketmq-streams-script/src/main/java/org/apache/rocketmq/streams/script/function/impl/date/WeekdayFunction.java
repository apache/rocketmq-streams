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
public class WeekdayFunction {
    /**
     * 获取当前周第几天
     *
     * @param message
     * @param context
     * @param datetime
     * @return
     */
    @FunctionMethod(value = "weekday", alias = "dayofweek", comment = "获取星期几")
    public Integer weekday(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
        @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        Integer day = null;
        datetime = FunctionUtils.getValueString(message, context, datetime);
        format = FunctionUtils.getValueString(message, context, format);
        if (datetime == null) {
            return day;
        }
        Date date1 = DateUtil.parse(datetime, format);
        return DateUtil.getDayOfWeek(date1) - 1;
    }

    @FunctionMethod(value = "weekday", alias = "dayofweek", comment = "获取星期几")
    public Integer weekday(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime) {
        return weekday(message, context, datetime, "'" + DateUtil.DEFAULT_FORMAT + "'");
    }
}
