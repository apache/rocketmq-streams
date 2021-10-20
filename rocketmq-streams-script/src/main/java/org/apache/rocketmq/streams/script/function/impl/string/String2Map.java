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

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class String2Map {
    @FunctionMethod(value = "STR_TO_MAP",alias = "str_to_map", comment = "字符串解析成map")
    public Map<String,String> string2Map(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String str) {
        String ori = FunctionUtils.getValueString(message, context, str);
        if (StringUtil.isEmpty(ori)) {
            return null;
        }
       String[] values= ori.split(",");
        Map<String,String> result=new HashMap<>();
        for(String value:values){
            String[] kv=value.split("=");
            if(kv.length!=2){
                throw new RuntimeException("can not parse, "+ori);
            }
            result.put(kv[0],kv[1]);
        }
        return result;
    }
}
