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
package org.apache.rocketmq.streams.filter;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.junit.Test;

public class FilterComponentTest {
    @Test
    public void testFilter() {
        FilterComponent filterComponent = FilterComponent.getInstance();
        Rule rule = filterComponent.createRule("namespace", "rulename", "(ip,==,1.2.2.3)&((uid,=,1224)|(vmip,=,1.1.11.1))", "uid;int");
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ip", "1.2.2.3");
        jsonObject.put("uid", 1224);
        jsonObject.put("vmip", "1.1.1.1");
        List<Rule> ruleList=filterComponent.excuteRule(jsonObject, rule);
        System.out.println(ruleList.size());
    }
}
