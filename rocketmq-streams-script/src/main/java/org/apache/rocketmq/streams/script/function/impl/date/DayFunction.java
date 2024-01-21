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

import java.util.Calendar;
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class DayFunction {

    @FunctionMethod(value = "dayBegin", alias = "begin", comment = "获取指定日期的开始时间")
    public Long dayBegin(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表往前推几天,0表示当天的时间") String datepart) {
        String datepartStr = FunctionUtils.getValueString(message, context, datepart);
        Date date = new Date();
        Calendar dateStart = Calendar.getInstance();
        dateStart.setTime(date);
        dateStart.set(Calendar.HOUR_OF_DAY, 0);
        dateStart.set(Calendar.MINUTE, 0);
        dateStart.set(Calendar.SECOND, 0);
        int dayTem = Integer.parseInt(datepartStr);
        dateStart.add(Calendar.DAY_OF_MONTH, -dayTem);
        return dateStart.getTimeInMillis();

    }

    @FunctionMethod(value = "dayEnd", alias = "end", comment = "获取指定日期的结束时间")
    public Long dayEnd(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表往前推几天,0表示当天的时间") String datepart) {
        String datepartStr = FunctionUtils.getValueString(message, context, datepart);
        Date date = new Date();
        Calendar dateEnd = Calendar.getInstance();
        dateEnd.setTime(date);
        dateEnd.set(Calendar.HOUR_OF_DAY, 23);
        dateEnd.set(Calendar.MINUTE, 59);
        dateEnd.set(Calendar.SECOND, 59);
        int dayTem = Integer.parseInt(datepartStr);
        dateEnd.add(Calendar.DAY_OF_MONTH, -dayTem);
        return dateEnd.getTimeInMillis();
    }
}
