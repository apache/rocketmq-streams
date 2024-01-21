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
public class SoundxFunction {

    /**
     * 将普通字符串转换成soundex字符串
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "soundx", comment = "将普通字符串转换成soundex字符串")
    public String soundx(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "转化的字段代表列字段或常量名") String fieldName) {
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori)) {
            return null;
        }
        return soundex(ori);
    }

    private String soundex(String s) {
        char[] x = s.toUpperCase().toCharArray();
        char firstLetter = x[0];

        // convert letters to numeric code
        for (int i = 0; i < x.length; i++) {
            switch (x[i]) {

                case 'B':
                case 'F':
                case 'P':
                case 'V':
                    x[i] = '1';
                    break;

                case 'C':
                case 'G':
                case 'J':
                case 'K':
                case 'Q':
                case 'S':
                case 'X':
                case 'Z':
                    x[i] = '2';
                    break;

                case 'D':
                case 'T':
                    x[i] = '3';
                    break;

                case 'L':
                    x[i] = '4';
                    break;

                case 'M':
                case 'N':
                    x[i] = '5';
                    break;

                case 'R':
                    x[i] = '6';
                    break;

                default:
                    x[i] = '0';
                    break;
            }
        }
        // remove duplicates
        String output = "" + firstLetter;
        for (int i = 1; i < x.length; i++) {
            if (x[i] != x[i - 1] && x[i] != '0') {
                output += x[i];
            }
        }

        // pad with 0's or truncate
        output = output + "0000";
        return output.substring(0, 4);
    }

}
