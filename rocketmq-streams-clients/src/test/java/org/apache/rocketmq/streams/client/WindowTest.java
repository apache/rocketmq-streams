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
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.functions.ForEachFunction;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.junit.Test;

public class WindowTest implements Serializable {

    @Test
    public void testWindow() {
        StreamBuilder.dataStream("namespace", "name")
            .fromFile("/Users/duheng/project/opensource/sls_100.txt", false)
            .map((MapFunction<JSONObject, String>)message -> JSONObject.parseObject(message))
            .window(TumblingWindow.of(Time.seconds(5)))
            .groupBy("ProjectName", "LogStore")
            .setLocalStorageOnly(true)
            .count("total")
            .sum("OutFlow", "OutFlow")
            .sum("InFlow", "InFlow")
            .toDataSteam()
            .forEach(new ForEachFunction<JSONObject>() {
                protected int sum = 0;

                @Override
                public void foreach(JSONObject o) {
                    int total = o.getInteger("total");
                    sum = sum + total;
                    o.put("sum(total)", sum);
                }
            }).toPrint().start();

    }

    //    @Test
    //    public void testWindowFromMetaq() throws InterruptedException {
    //        String topic = "TOPIC_DIPPER_SYSTEM_MSG_4";
    //        StreamBuilder.dataStream("namespace", "name")
    //            .fromFile("/Users/yuanxiaodong/chris/sls_100.txt", true)
    //            .toRocketmq(topic)
    //            .asyncStart();
    //
    //        StreamBuilder.dataStream("namespace", "name1")
    //            .fromRocketmq(topic, "chris", true)
    //            .window(TumblingWindow.of(Time.seconds(5)))
    //            .groupby("ProjectName", "LogStore")
    //            .setLocalStorageOnly(true)
    //            .count("total")
    //            .sum("OutFlow", "OutFlow")
    //            .sum("InFlow", "inflow")
    //            .toDataSteam()
    //            .forEach(new ForEachFunction<JSONObject>() {
    //                protected int sum = 0;
    //
    //                @Override
    //                public void foreach(JSONObject o) {
    //                    int total = o.getInteger("total");
    //                    sum = sum + total;
    //                    o.put("sum(total)", sum);
    //                }
    //            }).toPrint().start();
    //    }

}
