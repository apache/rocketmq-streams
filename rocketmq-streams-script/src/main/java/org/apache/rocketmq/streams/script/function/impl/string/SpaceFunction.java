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
package org.apache.rocketmq.streams.script.function.impl.string;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class SpaceFunction {

    /**
     * 空格字符串函数，返回长度为n的字符串
     *
     * @param message
     * @param context
     * @param countStr
     * @return
     */
    @FunctionMethod(value = "space", comment = "空格字符串函数，返回长度为n的字符串")
    public String space(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "空格数量") String countStr) throws Exception {
        StringBuilder sb = new StringBuilder();
        if (StringUtil.isEmpty(countStr.toString())) {
            return null;
        }
        Integer count = FunctionUtils.getValueInteger(message, context, countStr);
        if (StringUtil.isEmpty(count.toString())) {
            return null;
        }
        if (count > 200000000000L) {
            throw new Exception("数据长度太长");
        }
        for (long i = 1; i < count; i++) {
            sb.append(" ");
        }
        return sb.toString();
    }

}