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
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

import java.util.Date;

@Function
public class NowFuntion {
    @FunctionMethod(value = "now", alias = "newdate", comment = "获取当前时间")
    public String now(IMessage message, FunctionContext context) {

        return DateUtil.format(new Date());
    }

    @FunctionMethod(value = "now", alias = "newdate", comment = "获取当前时间，按指定格式输出")
    public String now(IMessage message, FunctionContext context, @FunctionParamter("代表输出时间格式的字段名或常量") String format) {
        if (StringUtil.isEmpty(format)) {
            return now(message, context);
        }
        format = FunctionUtils.getValueString(message, context, format);
        return DateUtil.format(new Date(), format);
    }
}
