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

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class KeyValueFunction {

    /**
     * 将srcStr（源字符串）按split1分成“key-value”对，按split2将key-value对分开，返回“key”所对应的value
     *
     * @param message
     * @param context
     * @param str
     * @param split1
     * @param split2
     * @param key
     * @return
     */
    @FunctionMethod(value = "keyvalue", alias = "kv", comment =
        "将srcStr（源字符串）按split1分成“key-value”对，按split2将key-value对分开，返回“key”所对应的value")
    public String keyvalue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str,
        @FunctionParamter(value = "string", comment = "第一分隔符，字段名或常量") String split1,
        @FunctionParamter(value = "string", comment = "第二分隔符，字段名或常量") String split2,
        @FunctionParamter(value = "string", comment = "待返回value对应的key，字段名或常量") String key) {
        JSONObject jsonObject = strtomap(message, context, str, split1, split2);
        return jsonObject.getString(key);
    }

    /**
     * 将srcStr（源字符串）按split1分成“key-value”对，按split2将key-value对分开，返回“key”所对应的value
     *
     * @param message
     * @param context
     * @param str
     * @param key
     * @return
     */
    @FunctionMethod(value = "keyvalue", alias = "kv", comment = "将srcStr（源字符串）按;"
        + "分成“key-value”对，按:将key-value对分开，返回“key”所对应的value")
    public String keyvalue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String str,
        @FunctionParamter(value = "string", comment = "待返回value对应的key，字段名或常量") String key) {
        return keyvalue(message, context, str, ";", ":", key);
    }

    /**
     * 将srcStr（源字符串）按split1分成“key-value”对，按split2将key-value对分开，返回“key”所对应的value
     *
     * @param message
     * @param context
     * @param str
     * @return
     */
    @FunctionMethod(value = "jsoncreate", alias = "str2json", comment = "解析'name'='yuanxiaodong','age'=18的字符串为json")
    public JSONObject strtomap(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "格式为'name'='yuanxiaodong','age'=18的字符串，支持字段名和常量") String str) {
        return strtomap(message, context, str, ",", "=");
    }

    /**
     * 将srcStr（源字符串）按split1分成“key-value”对，按split2将key-value对分开，返回“key”所对应的value
     *
     * @param message
     * @param context
     * @param str
     * @param split1
     * @param split2
     * @return
     */
    @FunctionMethod(value = "strtomap", alias = "str2json", comment = "解析'name'='yuanxiaodong','age'=18的字符串为json")
    public JSONObject strtomap(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "格式为'name'='yuanxiaodong','age'=18的字符串，支持字段名和常量") String str,
        @FunctionParamter(value = "string", comment = "第一分隔符，字段名或常量") String split1,
        @FunctionParamter(value = "string", comment = "第二分隔符，字段名或常量") String split2) {
        JSONObject result = null;
        str = FunctionUtils.getValueString(message, context, str);
        split1 = FunctionUtils.getValueString(message, context, split1);
        split2 = FunctionUtils.getValueString(message, context, split2);
        if (str == null || split1 == null || split2 == null) {
            return null;
        }

        JSONObject mapTem = new JSONObject();
        String strs[] = str.split(split1);
        for (String str1 : strs) {
            String strsTem[] = str1.split(split2);
            String key = FunctionUtils.getValueString(message, context, strsTem[0]);
            Object value = FunctionUtils.getValue(message, context, strsTem[1]);
            mapTem.put(key, value);
        }
        result = mapTem;
        return result;
    }
}
