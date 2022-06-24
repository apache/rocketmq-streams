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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.client.transform.SplitStream;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.functions.FilterFunction;
import org.apache.rocketmq.streams.common.functions.FlatMapFunction;
import org.apache.rocketmq.streams.common.functions.SplitFunction;
import org.junit.Test;

public class SplitTest implements Serializable {

    @Test
    public void testOperator() throws InterruptedException {
        DataStream stream = (StreamBuilder.dataStream("namespace", "name").fromFile("/Users/yuanxiaodong/chris/sls_1000.txt").flatMap(new FlatMapFunction<JSONObject, String>() {
            @Override
            public List<JSONObject> flatMap(String message) throws Exception {
                List<JSONObject> msgs = new ArrayList<>();
                for (int i = 0; i < 10; i++) {
                    JSONObject msg = JSONObject.parseObject(message);
                    msg.put("index", i);
                    msgs.add(msg);
                }
                return msgs;
            }
        }).filter(message -> ((JSONObject) message).getString("Project") == null));

        SplitStream splitStream = stream.split(new SplitFunction<JSONObject>() {
            @Override
            public String split(JSONObject o) {
                if (o.getInteger("age") < 18) {
                    return "children";
                } else if (o.getInteger("age") >= 18) {
                    return "adult";
                }
                return null;
            }
        });

        DataStream children = splitStream.select("children");
        DataStream adult = splitStream.select("adult");
        children.union(adult).join("dburl", "dbUserName", "dbPassowrd", "tableNameOrSQL", 5).setCondition("(name,==,name)").toDataStream().window(TumblingWindow.of(Time.seconds(5))).groupBy("ProjectName", "LogStore").setLocalStorageOnly(true).count("total").sum("OutFlow", "OutFlow").sum("InFlow", "InFlow").toDataStream().toPrint().asyncStart();
        while (true) {
            Thread.sleep(1000);
        }

    }

    @Test
    public void testDim() {
        DataStream stream = (StreamBuilder.dataStream("namespace", "name").fromFile("/Users/yuanxiaodong/chris/sls_1000.txt").filter(new FilterFunction<JSONObject>() {

            @Override
            public boolean filter(JSONObject value) throws Exception {
                if (value.getString("ProjectName") == null || value.getString("LogStore") == null) {
                    return true;
                }
                return false;
            }
        })).join("dburl", "dbUserName", "dbPassowrd", "tableNameOrSQL", 5).setCondition("(name,==,name)").toDataStream().selectFields("name", "age", "address").toPrint();
        stream.start();

    }
}
