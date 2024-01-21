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
package org.apache.rocketmq.streams.filter.service;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.common.interfaces.IFilterService;
import org.apache.rocketmq.streams.filter.operator.Rule;

/**
 * 规则引擎服务，执行规则
 */
public interface IRuleEngineService extends IFilterService<Rule> {

    /**
     * 执行规则，返回触发的规则
     *
     * @param message
     * @param rules
     * @return
     */
    List<Rule> excuteRule(JSONObject message, List<Rule> rules);

}
