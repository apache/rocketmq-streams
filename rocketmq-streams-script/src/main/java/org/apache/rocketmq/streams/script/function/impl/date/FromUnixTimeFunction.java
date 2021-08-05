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
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class FromUnixTimeFunction {

    /**
     * @param message
     * @param context
     * @param unixtime
     * @return
     */
    @FunctionMethod(value = "fromunixtime", comment = "把unixtime转换成自定义日期格式")
    public String formUnixTime(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "string", comment = "代表unixtime的字段名或常量") String unixtime,
                               @FunctionParamter(value = "string", comment = "代表日期格式的字段名或常量") String format) {
        Timestamp timestamp = null;
        if (unixtime == null) {
            return null;
        }
        format = FunctionUtils.getValueString(message, context, format);
        String value = FunctionUtils.getValueString(message, context, unixtime);
        if (FunctionUtils.isLong(value)) {
            Long unix = Long.parseLong(value);
            Date date = new Date(unix * 1000);
            value = DateUtil.format(date, format);
        }

        return value;
    }

    @FunctionMethod(value = "fromunixtime", comment = "把unixtime转换成标准格式")
    public String formUnixTime(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "string", comment = "代表unixtime的字段名或常量") String unixtim) {

        return formUnixTime(message, context, unixtim, "'" + DateUtil.DEFAULT_FORMAT + "'");
    }

    public static void main(String[] args) {
        Date date = new Date(1608624595 * 1000);
        String value = DateUtil.format(date, DateUtil.DEFAULT_FORMAT);
        System.out.println(value);
    }
}