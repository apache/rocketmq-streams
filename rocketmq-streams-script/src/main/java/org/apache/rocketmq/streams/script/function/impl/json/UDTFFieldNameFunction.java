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
package org.apache.rocketmq.streams.script.function.impl.json;


import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class UDTFFieldNameFunction {

    @FunctionMethod(value = "addAliasForNewField",comment = "获取msg中的json数据")
    public Object addAliasForNewField(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String fieldName,String alias,int i){
        fieldName=FunctionUtils.getConstant(fieldName);
        alias=FunctionUtils.getConstant(alias);
        Object object=message.getMessageBody().get(fieldName);
        if(message.getMessageBody().containsKey("f"+i)) {
            object=message.getMessageBody().get("f"+i);
            message.getMessageBody().put(alias+fieldName,object);
        }
        else if(message.getMessageBody().containsKey(fieldName)&&!message.getMessageBody().containsKey("f"+i)){
            message.getMessageBody().put(alias+fieldName,object);
        }
        return object;
    }


}
