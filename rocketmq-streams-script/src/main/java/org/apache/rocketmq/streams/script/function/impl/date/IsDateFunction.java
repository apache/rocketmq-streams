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

@Function
public class IsDateFunction {

    /**
     * 判断传递的数据是否是指定格式的时间字符串
     *
     * @param message
     * @param context
     * @param date
     * @param format
     * @return
     */
    @FunctionMethod(value = "isdate", alias = "isValidTime", comment = "判断传递的数据是否是指定格式的时间字符串")
    public boolean isDate(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String date,
                          @FunctionParamter(value = "string", comment = "代表时间格式的字段名或常量") String format) {
        boolean flag = false;
        date = FunctionUtils.getValueString(message, context, date);
        format = FunctionUtils.getValueString(message, context, format);
        return DateUtil.isValidTime(date, format);
    }

    @FunctionMethod(value = "isdate", alias = "isValidTime", comment = "字符串是否是格式为’yyyy-MM-dd HH:mm:ss‘的时间字符串")
    public boolean isDate(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String date) {
        return isDate(message, context, date, "'" + DateUtil.DEFAULT_FORMAT + "'");
    }
}
