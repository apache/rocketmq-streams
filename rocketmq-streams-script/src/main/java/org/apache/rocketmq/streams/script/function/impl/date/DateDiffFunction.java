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

/**
 * * 计算从enddate到startdate两个时间的天数差值， 日期格式可以是yy-MM-dd HH:mm:ss或yy-MM-dd或timestamp，返回整数， 若有参数为null或解析错误，返回null。
 */
@Function
public class DateDiffFunction {

    /**
     * 计算从enddate到startdate两个时间的天数差值， 日期格式可以是yy-MM-dd HH:mm:ss或yy-MM-dd或timestamp，返回整数， 若有参数为null或解析错误，返回null。
     *
     * @param message
     * @param context
     * @param startdate
     * @param enddate
     * @return
     */
    @FunctionMethod(value = "dateDiff", comment = "计算从enddate到startdate两个时间的天数差值")
    public Long dateDiff(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String startdate,
                         @FunctionParamter(value = "String", comment = "代表时间格式化格式") String enddate) {
        startdate = FunctionUtils.getValueString(message, context, startdate);
        enddate = FunctionUtils.getValueString(message, context, enddate);
        Date end = DateUtil.parse(enddate, DateUtil.DEFAULT_FORMAT);
        Date start = DateUtil.parse(startdate, DateUtil.DEFAULT_FORMAT);
        if (end == null || start == null) {
            return null;
        }
        return DateUtil.dateDiff(end, start);
    }

    /**
     * 计算从enddate到startdate两个时间的天数差值， 日期格式可以是yy-MM-dd HH:mm:ss或yy-MM-dd或timestamp，返回整数， 若有参数为null或解析错误，返回null。
     *
     * @param message
     * @param context
     * @param startdate
     * @param enddate
     * @return
     */
    @FunctionMethod(value = "dateDiff", comment = "计算从enddate到startdate两个时间的天数差值")
    public Long dateDiff(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "long", comment = "代表时间的字段名或常量") long startdate,
                         @FunctionParamter(value = "long", comment = "代表时间格式化格式") long enddate) {

        Date end = new Date(enddate);
        Date start = new Date(startdate);
        if (end == null || start == null) {
            return null;
        }
        return DateUtil.dateDiff(end, start);
    }

    /**
     * 计算从enddate到startdate两个时间的天数差值， 日期格式可以是yy-MM-dd HH:mm:ss或yy-MM-dd或timestamp，返回整数， 若有参数为null或解析错误，返回null。
     *
     * @param message
     * @param context
     * @param startdate
     * @param enddate
     * @return
     */
    @FunctionMethod(value = "dateDiff", comment = "计算从enddate到startdate两个时间的天数差值")
    public Long dateDiff(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String startdate,
                         @FunctionParamter(value = "long", comment = "代表时间格式化格式") long enddate) {

        Date end = new Date(enddate);
        Date start = DateUtil.parse(startdate, DateUtil.DEFAULT_FORMAT);
        if (end == null || start == null) {
            return null;
        }
        return DateUtil.dateDiff(end, start);
    }

    /**
     * 计算从enddate到startdate两个时间的天数差值， 日期格式可以是yy-MM-dd HH:mm:ss或yy-MM-dd或timestamp，返回整数， 若有参数为null或解析错误，返回null。
     *
     * @param message
     * @param context
     * @param startdate
     * @param enddate
     * @return
     */
    @FunctionMethod(value = "dateDiff", comment = "计算从enddate到startdate两个时间的天数差值")
    public Long dateDiff(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "long", comment = "代表时间的字段名或常量") long startdate,
                         @FunctionParamter(value = "string", comment = "代表时间格式化格式") String enddate) {

        Date start = new Date(startdate);
        Date end = DateUtil.parse(enddate, DateUtil.DEFAULT_FORMAT);
        if (end == null || start == null) {
            return null;
        }
        return DateUtil.dateDiff(end, start);
    }

}
