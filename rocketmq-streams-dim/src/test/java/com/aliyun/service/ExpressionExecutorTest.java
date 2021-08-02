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
package com.aliyun.service;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.expression.SimpleExpression;
import org.apache.rocketmq.streams.filter.FilterComponent;
import org.junit.Test;

import java.io.File;

public class ExpressionExecutorTest {
    private static final String CREDIBLE_PROPERTIES = "credible" + File.separator + "credible.properties";
    private FilterComponent filterComponent;

    private String namespace = "yundun.credible.net.vistor";
    private String ruleNameSpace = "credible.rule.net.vistor";
    private String selectorName = "credible.selector.net.vistor";
    private String selectorExpression = "(host_uuid,=,0a2153e2-e45c-403f-8d5f-d811f400c3fb)";
    private String procWriteList = "credible.namelist.proc";
    private String netWriteList = "credible.namelist.net.vistor";

    private String ruleExpression =
        "(proc_path,in_resouce," + namespace + "->" + procWriteList + ")&(inner_message,not_in_expression_resouce,'"
            + namespace + "->" + netWriteList
            + "->(visitor_ip,=,dest_ip)&(visitor_port,=,dest_port)&(proc_path,=,program_path)')";

    //    @Test
    //    public void parseExpression() {
    //        List<Expression> expressions = new ArrayList<>();
    //        List<RelationExpression> relationExpressions = new ArrayList<>();
    //        Expression expression = ExpressionBuilder.createExpression("namespace", ruleExpression,
    // expressions,
    //            relationExpressions);
    //    }

    public ExpressionExecutorTest() {
        //        FilterComponent filterComponent= new FilterComponent();
        //        filterComponent.init(CREDIBLE_PROPERTIES);
        //        filterComponent.start(null);
        //        this.filterComponent=filterComponent;
    }

    @Test
    public void testExecutor() {
        System.out.println("hello wolrd");
        JSONObject msg = new JSONObject();
        msg.put("ip", "1.1.1.1");
        boolean match = ExpressionBuilder.executeExecute(new SimpleExpression("ip", "=", "1.1.1.1"), msg);
        System.out.println(match);
    }

    @Test
    public void testRelationExecutor() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("ip", "1.2.2.3");
        jsonObject.put("uid", "1224");
        jsonObject.put("vmip", "1.1.1.1");

        boolean value =
            ExpressionBuilder.executeExecute("namespace", "(ip,=,1.2.2.3)&((uid,=,12214)|(vmip,=,1.1.11.1))",
                jsonObject);
        System.out.println(value);
    }
}
