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

package org.apache.rocketmq.streams.client;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.filter.FilterComponent;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class FilterTest {
    @Test
    public void testFilter(){
        JSONObject msg=new JSONObject();
        msg.put("name","chris");
        msg.put("age",18);
        Rule rule= ExpressionBuilder.createRule("tmp","tmp","(name,==,chris)&(age,>=,18)");
        FilterComponent filterComponent=FilterComponent.getInstance();
        List<Rule> fireRules=filterComponent.excuteRule(msg,rule);
        assertTrue(fireRules.size()==1);
    }

    @Test
    public void testFilter2(){
        JSONObject msg=new JSONObject();
        msg.put("name","chris");
        msg.put("age",18);
        boolean result= ExpressionBuilder.executeExecute("tmp","(name,==,chris)&(age,>,int,18)",msg);
        assertTrue(!result);
    }
}
