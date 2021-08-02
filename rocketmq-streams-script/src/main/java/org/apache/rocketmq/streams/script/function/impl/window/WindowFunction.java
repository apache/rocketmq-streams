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
package org.apache.rocketmq.streams.script.function.impl.window;

import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class WindowFunction {

    /**
     * window start key used by TumbleFunction and HopFunction
     */
    public static final String WINDOW_START = "WINDOW_START_TIME";

    /**
     * window end key used by TumbleFunction and HopFunction
     */
    public static final String WINDOW_END = "WINDOW_END";

    @FunctionMethod(value = "window_start", comment = "获取滚动/滑动窗口的起始时间")
    public String windowStart(IMessage message, FunctionContext context) {
        return getWindowStartTime(message, context);
    }

    @FunctionMethod(value = "window_end", comment = "获取滚动/滑动窗口的结束时间")
    public String windowEnd(IMessage message, FunctionContext context) {
        return getWindowEndTime(message, context);
    }

    private String getWindowStartTime(IMessage message, FunctionContext context) {
        String startTime = FunctionUtils.getValueString(message, context, WINDOW_START);
        if (StringUtil.isEmpty(startTime)) {
            return null;
        }
        return startTime;
    }

    private String getWindowEndTime(IMessage message, FunctionContext context) {
        String endTime = FunctionUtils.getValueString(message, context, WINDOW_END);
        if (StringUtil.isEmpty(endTime)) {
            return null;
        }
        return endTime;
    }
}
