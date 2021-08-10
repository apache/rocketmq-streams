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
public class UnixTimeStampFunction {

    /**
     * 将当前日期转换为unix的时间戳
     *
     * @param message
     * @param context
     * @param datetime
     * @return
     */
    @FunctionMethod(value = "unixtimestamp", alias = "unixtime", comment = "获取unix时间")
    public Long unixStam(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                         @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        Long unixStap = null;
        datetime = FunctionUtils.getValueString(message, context, datetime);
        if (datetime == null) {
            return unixStap;
        }
        format = FunctionUtils.getValueString(message, context, format);
        try {
            Date date = DateUtil.parse(datetime, format);
            return date.getTime() / 1000;
        } catch (Exception e) {
            //TODO log
            return null;
        }
    }

    /**
     * 将传入的日期转换为unix时间戳
     *
     * @param message
     * @param context
     * @param datetime
     * @return
     */

    @FunctionMethod(value = "unixtimestamp", alias = "unixtime", comment = "获取unix时间")
    public Long unixStam(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime) {
        //将"'"+DateUtil.DEFAULT_FORMAT+"'"两边的引号去除，在下面DateUtil.parse解析的时候会由于引号解析失败。
        return unixStam(message, context, datetime, DateUtil.DEFAULT_FORMAT);

    }
}
