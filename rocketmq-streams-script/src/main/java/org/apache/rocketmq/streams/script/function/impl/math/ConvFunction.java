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
package org.apache.rocketmq.streams.script.function.impl.math;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class ConvFunction {

    /**
     * 进制转换函数
     *
     * @param message
     * @param context
     * @param input
     * @param from_base
     * @param to_base
     * @return
     */

    @FunctionMethod(value = "conv", alias = "conv", comment = "进制转换函数")
    public String conv(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "String", comment = "要转换的字段名称或常量值") String input,
                       @FunctionParamter(value = "String", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") String from_base,
                       @FunctionParamter(value = "String", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") String to_base) {
        String result = null;
        String inputTem = FunctionUtils.getValueString(message, context, input);
        Integer formTem = Integer.parseInt(FunctionUtils.getValueString(message, context, from_base));
        Integer toTem = Integer.parseInt(FunctionUtils.getValueString(message, context, to_base));
        if (inputTem == null || formTem == null || toTem == null) {
            return result;
        }
        result = conversionUtil(formTem, toTem, inputTem);
        return result;
    }

    /**
     * 进制转换函数
     *
     * @param message
     * @param context
     * @param input
     * @param from_base
     * @param to_base
     * @return
     */

    @FunctionMethod(value = "conv", alias = "conv", comment = "进制转换函数")
    public String conv(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "Double", comment = "要转换的字段名称或常量值") Double input,
                       @FunctionParamter(value = "String", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") String from_base,
                       @FunctionParamter(value = "String", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") String to_base) {
        String result = null;
        Integer formTem = Integer.parseInt(FunctionUtils.getValueString(message, context, from_base));
        Integer toTem = Integer.parseInt(FunctionUtils.getValueString(message, context, to_base));
        if (input == null || formTem == null || toTem == null) {
            return result;
        }
        result = conversionUtil(formTem, toTem, input.toString());
        return result;
    }

    /**
     * 进制转换函数
     *
     * @param message
     * @param context
     * @param input
     * @param from_base
     * @param to_base
     * @return
     */

    @FunctionMethod(value = "conv", alias = "conv", comment = "进制转换函数")
    public String conv(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "Double", comment = "要转换的字段名称或常量值") Double input,
                       @FunctionParamter(value = "Double", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") Double from_base,
                       @FunctionParamter(value = "String", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") String to_base) {
        String result = null;
        Integer toTem = Integer.parseInt(FunctionUtils.getValueString(message, context, to_base));
        if (input == null || from_base == null || toTem == null) {
            return result;
        }
        result = conversionUtil(from_base.intValue(), toTem, input.toString());
        return result;
    }

    /**
     * 进制转换函数
     *
     * @param message
     * @param context
     * @param input
     * @param from_base
     * @param to_base
     * @return
     */

    @FunctionMethod(value = "conv", alias = "conv", comment = "进制转换函数")
    public String conv(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "Double", comment = "要转换的字段名称或常量值") Double input,
                       @FunctionParamter(value = "Double", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") Double from_base,
                       @FunctionParamter(value = "Double", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") Double to_base) {
        String result = null;
        if (input == null || from_base == null || to_base == null) {
            return result;
        }
        result = conversionUtil(from_base.intValue(), to_base.intValue(), input.toString());
        return result;
    }

    /**
     * 进制转换函数
     *
     * @param message
     * @param context
     * @param input
     * @param from_base
     * @param to_base
     * @return
     */

    @FunctionMethod(value = "conv", alias = "conv", comment = "进制转换函数")
    public String conv(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "String", comment = "要转换的字段名称或常量值") String input,
                       @FunctionParamter(value = "Double", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") Double from_base,
                       @FunctionParamter(value = "String", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") String to_base) {
        String result = null;
        String inputTem = FunctionUtils.getValueString(message, context, input);
        Integer toTem = Integer.parseInt(FunctionUtils.getValueString(message, context, to_base));
        if (inputTem == null || from_base == null || toTem == null) {
            return result;
        }
        result = conversionUtil(from_base.intValue(), toTem, inputTem);
        return result;
    }

    @FunctionMethod(value = "conv", alias = "conv", comment = "进制转换函数")
    public String conv(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "String", comment = "要转换的字段名称或常量值") String input,
                       @FunctionParamter(value = "String", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") String from_base,
                       @FunctionParamter(value = "Double", comment = "以十进制表示的进制的值，可接受的的值为2，8，10，16") Double to_base) {
        String result = null;
        String inputTem = FunctionUtils.getValueString(message, context, input);
        Integer formTem = Integer.parseInt(FunctionUtils.getValueString(message, context, from_base));
        if (inputTem == null || formTem == null || to_base == null) {
            return result;
        }
        result = conversionUtil(formTem, to_base.intValue(), inputTem);
        return result;
    }

    private String conversionUtil(int formTem, int toTem, String inputTem) {
        String result = "";
        if (formTem == 2) {
            switch (toTem) {
                case 2:
                    result = inputTem;
                    break;
                case 8:
                    result = Integer.toOctalString(Integer.parseInt(inputTem, 2));
                    break;
                case 10:
                    result = Integer.valueOf(inputTem, 2).toString();
                    break;
                case 16:
                    result = Integer.toHexString(Integer.parseInt(inputTem, 2));
                    break;
            }
        }
        if (formTem == 8) {
            switch (toTem) {
                case 2:
                    result = Integer.toBinaryString(Integer.valueOf(inputTem, 8));
                    break;
                case 8:
                    result = inputTem;
                    break;
                case 10:
                    result = Integer.valueOf("inputTem", 8).toString();
                    break;
                case 16:
                    result = Integer.toHexString(Integer.valueOf(inputTem, 8));
                    break;
            }
        }
        if (formTem == 10) {
            Integer inputi = Integer.parseInt(inputTem);
            switch (toTem) {
                case 2:
                    result = Integer.toBinaryString(inputi);
                    break;
                case 8:
                    result = Integer.toOctalString(inputi);
                    break;
                case 10:
                    result = inputTem;
                    break;
                case 16:
                    result = Integer.toHexString(inputi);
                    break;
            }
        }
        if (formTem == 16) {
            switch (toTem) {
                case 2:
                    result = Integer.toBinaryString(Integer.valueOf(inputTem, 16));
                    ;
                    break;
                case 8:
                    result = Integer.toOctalString(Integer.valueOf(inputTem, 16));
                    break;
                case 10:
                    Integer.valueOf(inputTem, 16).toString();
                    break;
                case 16:
                    result = inputTem;
                    break;
            }
        }
        return result;
    }
}