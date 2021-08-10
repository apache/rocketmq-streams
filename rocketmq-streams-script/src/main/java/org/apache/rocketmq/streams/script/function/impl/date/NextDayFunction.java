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

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class NextDayFunction {

    @FunctionMethod(value = "nextday", comment = "")
    public String nextday(IMessage message, FunctionContext context, String datetime, String week) {
        String result = null;
        datetime = FunctionUtils.getValueString(message, context, datetime);
        if (datetime == null) {
            return result;
        }
        DateFormat dateFormat = DateUtil.getDateFormat("yyyy-MM-dd");
        Date date = null;
        try {
            date = dateFormat.parse(datetime);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        switch (week) {
            case "MO":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                break;
            case "MON":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
                break;
            case "TU":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY);
                break;
            case "TUE":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY);
                break;
            case "TH":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY);
                break;
            case "THU":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.THURSDAY);
                break;
            case "WE":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.WEDNESDAY);
                break;
            case "WED":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.WEDNESDAY);
                break;
            case "FR":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.FRIDAY);
                break;
            case "SA":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
                break;
            case "SAT":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.SATURDAY);
                break;
            case "SU":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
                break;
            case "SUN":
                cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
                break;

        }
        cal.setFirstDayOfWeek(Calendar.MONDAY);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.TUESDAY);
        cal.add(Calendar.DATE, 7);
        result = dateFormat.format(cal.getTime());
        return result;
    }
}
