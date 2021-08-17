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
package org.apache.rocketmq.streams.filter.engine;

import java.util.List;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.filter.context.RuleMessage;
import org.apache.rocketmq.streams.filter.operator.Rule;

public interface IRuleEngine {

    /**
     * 把消息相关的上下文传递给规则引擎，规则引擎进行变量封装，资源检查，规则执行等操作
     *
     * @param message 包含变量相关信息和原始的message信息
     * @return 触发的规则
     */
    List<Rule> executeRule(RuleMessage message, List<Rule> rules);

    /**
     * 把消息相关的上下文传递给规则引擎，规则引擎进行变量封装，资源检查，规则执行等操作
     *
     * @param message 包含变量相关信息和原始的message信息
     * @return 触发的规则
     */
    List<Rule> executeRule(AbstractContext context, RuleMessage message, List<Rule> rules);

    /**
     * 把消息相关的上下文传递给规则引擎，规则引擎进行变量封装，资源检查，规则执行等操作
     *
     * @param message 包含变量相关信息和原始的message信息
     * @return 触发的规则
     */
    List<Rule> executeRuleWithoutAction(RuleMessage message, List<Rule> rules);

}
