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
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class InstrtFunction {

    @FunctionMethod(value = "contains", comment = "返回字符串str2在str1中的位置")
    public boolean contains(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str1,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str2) {
        Integer index = instr(message, context, str1, str2);
        return index > -1;
    }

    /**
     * 返回字符串str2在str1中的位置
     *
     * @param message
     * @param context
     * @param str1
     * @param str2
     * @return
     */
    @FunctionMethod(value = "instr", alias = "indexOf", comment = "返回字符串str2在str1中的位置")
    public Integer instr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str1,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str2) {
        Integer number = null;
        str1 = FunctionUtils.getValueString(message, context, str1);
        str2 = FunctionUtils.getValueString(message, context, str2);
        if (str2 == null || str1 == null) {
            return -1;
        }
        number = str1.indexOf(str2);
        return number;
    }

    /**
     * blink sql中的位置是从1开始的，所以在原有的位置上加1
     *
     * @param message
     * @param context
     * @param str1
     * @param str2
     * @return
     */
    @FunctionMethod(value = "blink_instr", alias = "blink_indexOf", comment = "返回字符串str2在str1中的位置")
    public Integer blinkInstr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str1,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str2) {
        int index = instr(message, context, str1, str2);
        return index + 1;
    }

    /**
     * 返回字符串str2在str1中的位置
     *
     * @param message
     * @param context
     * @param str1
     * @param str2
     * @return
     */
    @FunctionMethod(value = "instr", alias = "indexOf", comment = "返回字符串str2在str1中从index后的位置")
    public Integer instr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str1,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str2,
        @FunctionParamter(value = "string", comment = "起始位置，数字，字段名或常量") String positionStr) {
        Integer number = null;
        str1 = FunctionUtils.getValueString(message, context, str1);
        str2 = FunctionUtils.getValueString(message, context, str2);
        Integer position = FunctionUtils.getValueInteger(message, context, positionStr);
        if (str2 == null || str2 == null || position == null) {
            return number;
        }
        number = str1.indexOf(str2, position);
        return number;
    }

    /**
     * 返回字符串str2在str1中的位置
     *
     * @param message
     * @param context
     * @param str1
     * @param str2
     * @return
     */
    @FunctionMethod(value = "instr", alias = "indexOf", comment = "返回字符串str2在str1中从index后出现n次的位置")
    public Integer instr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str1,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str2,
        @FunctionParamter(value = "string", comment = "起始位置，数字，字段名或常量") String positionStr,
        @FunctionParamter(value = "string", comment = "出现的次数，数字，字段名或常量") String appearanceStr) {
        Integer number = null;
        str1 = FunctionUtils.getValueString(message, context, str1);
        str2 = FunctionUtils.getValueString(message, context, str2);
        Integer position = FunctionUtils.getValueInteger(message, context, positionStr);
        Integer appearance = FunctionUtils.getValueInteger(message, context, appearanceStr);
        if (str2 == null || str2 == null) {
            return number;
        }
        int i = 1;
        while (i <= appearance) {
            number = str1.indexOf(str2, position + 1);
            position = number;
            i++;
        }
        return number;
    }

}
