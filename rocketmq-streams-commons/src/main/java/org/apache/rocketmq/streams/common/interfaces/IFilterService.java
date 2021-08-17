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
package org.apache.rocketmq.streams.common.interfaces;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;

public interface IFilterService<T extends AbstractRule> {

    /**
     * 执行规则，返回触发的规则
     *
     * @param message 消息
     * @param rules   规则
     * @return 触发的规则
     */
    List<T> excuteRule(JSONObject message, T... rules);

    /**
     * 执行规则，返回触发的规则
     *
     * @param message 消息
     * @param rules   规则
     * @return 触发的规则
     */
    List<T> executeRule(IMessage message, AbstractContext context, T... rules);

    /**
     * create rule 根据表达式，创建一个规则
     *
     * @param namespace     命名空间
     * @param ruleName      规则名称
     * @param expressionStr (varName,functionName,value)&varName,functionName,datatypeName,value)
     * @param msgMetaInfo   如果变量的某些字段不是字符串，需要做下标注。如果是字符串，不用设置。 格式：fieldname;datatypename;isRequired，例：age;int;true。如isRequired是false， *                  最后部分可以省略，age;int，如果datatypename是string，且isRequired是false，可以只写age，或不设置
     * @return 规则
     */
    AbstractRule createRule(String namespace, String ruleName, String expressionStr, String... msgMetaInfo);

}
