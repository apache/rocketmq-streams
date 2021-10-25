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
package org.apache.rocketmq.streams.script.function.impl.between;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class BetweenFunction {

    @FunctionMethod(value = "between", comment = "比较a是否在b,c之间")
    public boolean betweem(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName,String betweenStart,String betweenEnd) {
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        betweenStart = FunctionUtils.getValueString(message, context, betweenStart);
        betweenEnd = FunctionUtils.getValueString(message, context, betweenEnd);
        if(fieldName==null||betweenStart==null||betweenEnd==null){
            return false;
        }
        if(FunctionUtils.isLong(fieldName)&&FunctionUtils.isLong(betweenStart)&&FunctionUtils.isLong(betweenEnd)){
            Long fieldValue=Long.valueOf(fieldName);
            Long startValue=Long.valueOf(betweenStart);
            Long endValue=Long.valueOf(betweenEnd);
            if(fieldValue>=startValue&&fieldValue<=endValue){
                return true;
            }
            return false;
        }

        if(FunctionUtils.isDouble(fieldName)&&FunctionUtils.isDouble(betweenStart)&&FunctionUtils.isDouble(betweenEnd)){
            Double fieldValue=Double.valueOf(fieldName);
            Double startValue=Double.valueOf(betweenStart);
            Double endValue=Double.valueOf(betweenEnd);
            if(fieldValue>=startValue&&fieldValue<=endValue){
                return true;
            }
            return false;
        }
        if(fieldName.compareTo(betweenStart)>=0&&fieldName.compareTo(betweenEnd)<=0){
            return true;
        }
        return false;
    }
    @FunctionMethod(value = "between_not",alias = "betweennot", comment = "比较a是否在b,c之间")
    public boolean notBetweem(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName,String betweenStart,String betweenEnd) {
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        betweenStart = FunctionUtils.getValueString(message, context, betweenStart);
        betweenEnd = FunctionUtils.getValueString(message, context, betweenEnd);
        if(fieldName==null||betweenStart==null||betweenEnd==null){
            return false;
        }
        return !betweem(message,context,fieldName,betweenStart,betweenEnd);
    }
}
