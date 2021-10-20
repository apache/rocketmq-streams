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
package org.apache.rocketmq.streams.script.function.impl.udtf;

import com.alibaba.fastjson.JSONObject;
import java.util.Iterator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class UDTFFunction {
    /**
     * 把udtf拆分的列加上列名，生成新的message
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "renameudtf", alias = "udtfrename", comment = "把udtf拆分的列加上列名，生成新的message")
    public void udtf(IMessage message, FunctionContext context,
                     @FunctionParamter(value = "array", comment = "对应的字段名") String... fieldNames) {
        JSONObject jsonObject = message.getMessageBody();
        final JSONObject temp = new JSONObject();
        temp.putAll(jsonObject);
        Iterator<String> it = temp.keySet().iterator();
        while (it.hasNext()) {
            String fieldName = it.next();
            Object value = temp.get(fieldName);
            if (fieldName.startsWith(FunctionType.UDTF.getName())) {
                String[] values = fieldName.split("\\.");
                String indexStr = values[1];
                Integer index = Integer.valueOf(indexStr);
                if (index < fieldNames.length) {
                    jsonObject.remove(fieldName);
                    jsonObject.put(FunctionUtils.getValueString(message, context, fieldNames[index]), value);
                }
            }
        }

    }
}
