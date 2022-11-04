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
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.functions.FilterFunction;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.junit.Test;

public class UnionTest implements Serializable {

    @Test
    public void testUnion() {
        DataStream leftStream = (StreamBuilder.dataStream("namespace", "name")
            .fromFile("/Users/yuanxiaodong/chris/sls_1000.txt")
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override public JSONObject map(JSONObject message) throws Exception {
                    message.put("left",true);
                    return message;
                }
            }));

        DataStream rightStream = (StreamBuilder.dataStream("namespace", "name2")
            .fromFile("/Users/yuanxiaodong/chris/sls_1000.txt")
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override public JSONObject map(JSONObject message) throws Exception {
                    message.put("rigth",true);
                    return message;
                }
            }));

        leftStream.union(rightStream).toPrint().start();
    }

    @Test
    public void testDim() {
        DataStream stream = (StreamBuilder.dataStream("namespace", "name")
            .fromFile("/Users/yuanxiaodong/chris/sls_1000.txt")
            .filter(new FilterFunction<JSONObject>() {

                @Override
                public boolean filter(JSONObject value) throws Exception {
                    if (value.getString("ProjectName") == null || value.getString("LogStore") == null) {
                        return true;
                    }
                    return false;
                }
            }))
            .join("dburl", "dbUserName", "dbPassowrd", "tableNameOrSQL", 5)
            .setCondition("(name,==,name)")
            .toDataSteam()
            .selectFields("name", "age", "address")
            .toPrint();

        stream.start();

    }
}
