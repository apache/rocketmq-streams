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

import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.script.context.FunctionContext;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;

import java.util.HashSet;
import java.util.Set;

import org.apache.rocketmq.streams.common.context.IMessage;

@Function
public class CharMatchCountFunction {

    @FunctionMethod(value = "charmatchcount", alias = "char_matchcount", comment = "用于计算str1中有多少个字符出现在str2中")
    public Integer charmatchcount(IMessage message, FunctionContext context,
                                  @FunctionParamter(value = "string", comment = "字段名或常量") String str1,
                                  @FunctionParamter(value = "string", comment = "字段名或常量") String str2) {
        Integer number = 0;
        String str1Tem = FunctionUtils.getValueString(message, context, str1);
        String str2Tem = FunctionUtils.getValueString(message, context, str2);
        char chars[] = str1Tem.toCharArray();
        Set<Character> set = new HashSet();
        for (int i = 0; i < chars.length; i++) {
            char charTem = chars[i];
            set.add(charTem);
        }
        char char2s[] = str2Tem.toCharArray();
        Set<Character> set2 = new HashSet();
        for (char c : char2s) {
            if (set.contains(c) && !set2.contains(c)) {
                number++;
                set2.add(c);
                continue;
            }
            set2.add(c);
        }

        return number;
    }

}
