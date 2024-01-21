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
package org.apache.rocketmq.streams.dim.function.script;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;

@Function
public class IntelligenceFunction {

    @FunctionMethod(value = "intelligence", alias = "qingbao")
    public void intelligence(IMessage message, FunctionContext context, String namespace, String nameListName, String intelligenceFieldName, String asName) {
        intelligenceInner(message, context, namespace, nameListName, intelligenceFieldName, asName, true);
    }

    @FunctionMethod(value = "left_join_intelligence", alias = "left_join_qingbao")
    public void intelligenceLeftJoin(IMessage message, FunctionContext context, String namespace, String nameListName, String intelligenceFieldName, String asName) {
        intelligenceInner(message, context, namespace, nameListName, intelligenceFieldName, asName, false);
    }

    public void intelligenceInner(IMessage message, FunctionContext context, String namespace, String nameListName, String intelligenceFieldName, String asName, boolean isInner) {
//        String key = FunctionUtils.getValueString(message, context, intelligenceFieldName);
//        namespace = FunctionUtils.getValueString(message, context, namespace);
//        nameListName = FunctionUtils.getValueString(message, context, nameListName);
//        ConfigurableComponent configurableComponent = ComponentCreator.getComponent(namespace, ConfigurableComponent.class);
//        AbstractIntelligenceCache intelligenceCache = configurableComponent.queryConfigurable(AbstractIntelligenceCache.TYPE, nameListName);
//        if (intelligenceCache == null) {
//            throw new RuntimeException("can not query intelligence. the namespace is " + namespace + ", the name is " + nameListName);
//        }
//        Map<String, Object> row = intelligenceCache.getRow(key);
//        if (row != null) {
//            asName = FunctionUtils.getValueString(message, context, asName);
//            if (StringUtil.isNotEmpty(asName)) {
//                asName = asName + ".";
//            } else {
//                asName = "";
//            }
//            for (Entry<String, Object> entry : row.entrySet()) {
//                String elementKey = asName + entry.getKey();
//                message.getMessageBody().put(elementKey, entry.getValue());
//            }
//        } else {
//            if (isInner) {
//                context.breakExecute();
//            }
//        }
    }

}