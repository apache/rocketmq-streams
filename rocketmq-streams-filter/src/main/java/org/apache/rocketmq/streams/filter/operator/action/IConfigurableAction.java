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
package org.apache.rocketmq.streams.filter.operator.action;

import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;

public interface IConfigurableAction<T> {

    /**
     * 根据规则上下文执行configurable组件的默认动作
     *
     * @param context 规则上下文，一条数据一个
     * @param rule    当前正在执行的规则
     * @return
     */
    T doAction(RuleContext context, Rule rule);

    /**
     * 验证configurable组件的输入参数是否有效
     *
     * @param context 规则上下文，一条数据一个
     * @param rule    当前正在执行的规则
     * @return
     */
    boolean volidate(RuleContext context, Rule rule);
}
