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
public class QuarterFunction {

    @FunctionMethod(value = "quarter", comment = "获取当前的季度值，用数字表示")
    public Integer quarter(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime) {
        return quarter(message, context, datetime, "'" + DateUtil.DEFAULT_FORMAT + "'");
    }

    /**
     * 获取当前日期所属季度
     *
     * @param message
     * @param context
     * @param datetime
     * @return
     */
    @FunctionMethod(value = "quarter", comment = "获取当前的季度值，用数字表示")
    public Integer quarter(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String datetime,
                           @FunctionParamter(value = "string", comment = "代表格式的字段名或常量") String format) {
        Integer quarter = null;

        datetime = FunctionUtils.getValueString(message, context, datetime);
        format = FunctionUtils.getValueString(message, context, format);
        if (datetime == null) {
            return quarter;
        }
        Date date1 = DateUtil.parse(datetime, format);
        int month = DateUtil.getMonth(date1);
        if (month >= 1 && month <= 3) {
            quarter = 1;
        }
        if (month >= 4 && month <= 6) {
            quarter = 2;
        }
        if (month >= 7 && month <= 9) {
            quarter = 3;
        }
        if (month >= 10 && month <= 12) {
            quarter = 4;
        }
        return quarter;
    }

}
