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
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.client.transform.window.SessionWindow;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemoryCache;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.functions.ForEachFunction;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.junit.Assert;
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

    @Test
    public void testSession() {
        //dataset
        List<String> behaviorList = new ArrayList<>();

        JSONObject userA = new JSONObject();
        userA.put("time", DateUtil.parse("2021-09-09 10:00:00"));
        userA.put("user", "userA");
        userA.put("movie", "movie001");
        userA.put("flag", 1);
        behaviorList.add(userA.toJSONString());

        userA = new JSONObject();
        userA.put("time", DateUtil.parse("2021-09-09 10:00:01"));
        userA.put("user", "userA");
        userA.put("movie", "movie002");
        userA.put("flag", 1);
        behaviorList.add(userA.toJSONString());

        JSONObject userB = new JSONObject();
        userB.put("time", DateUtil.parse("2021-09-09 10:00:00"));
        userB.put("user", "userB");
        userB.put("movie", "movie003");
        userB.put("flag", 1);
        behaviorList.add(userB.toJSONString());

        JSONObject userC = new JSONObject();
        userC.put("time", DateUtil.parse("2021-09-09 10:00:00"));
        userC.put("user", "userC");
        userC.put("movie", "movie004");
        userC.put("flag", 1);
        behaviorList.add(userC.toJSONString());

        userC = new JSONObject();
        userC.put("time", DateUtil.parse("2021-09-09 10:00:06"));
        userC.put("user", "userC");
        userC.put("movie", "movie005");
        userC.put("flag", 1);
        behaviorList.add(userC.toJSONString());

        String dataFilePath = "/tmp/behavior.txt";
        File dataFile = new File(dataFilePath);
        dataFile.deleteOnExit();
        try {
            FileUtils.writeLines(dataFile, behaviorList);
        } catch (Exception e) {
            e.printStackTrace();
        }

        String resultFilePath = "/tmp/behavior.txt.session";
        File resultFile = new File(resultFilePath);
        resultFile.deleteOnExit();

        StreamBuilder.dataStream("namespace", "session_test")
            .fromFile(dataFilePath, false)
            .map((MapFunction<JSONObject, String>) message -> JSONObject.parseObject(message))
            .window(SessionWindow.of(Time.seconds(5), "time"))
            .groupBy("user")
            .setLocalStorageOnly(true)
            .sum("flag", "count")
            .toDataSteam()
            .toFile(resultFilePath).start(true);

        try {
            Thread.sleep(1 * 60 * 1000);
            List<String> sessionList = FileUtils.readLines(new File(resultFilePath), "UTF-8");
            Map<String, List<Pair<Pair<String, String>, Integer>>> sessionMap = new HashMap<>(4);
            for (String line : sessionList) {
                JSONObject object = JSONObject.parseObject(line);
                String user = object.getString("user");
                String startTime = object.getString("start_time");
                String endTime = object.getString("end_time");
                Integer value = object.getIntValue("count");
                if (!sessionMap.containsKey(user)) {
                    sessionMap.put(user, new ArrayList<>());
                }
                sessionMap.get(user).add(Pair.of(Pair.of(startTime, endTime), value));
            }
            Assert.assertEquals(3, sessionMap.size());
            Assert.assertEquals(1, sessionMap.get("userA").size());
            Assert.assertEquals("2021-09-09 10:00:06", sessionMap.get("userA").get(0).getLeft().getRight());
            Assert.assertEquals(2, sessionMap.get("userC").size());
            Assert.assertEquals("2021-09-09 10:00:05", sessionMap.get("userC").get(0).getLeft().getRight());
            Assert.assertEquals("2021-09-09 10:00:06", sessionMap.get("userC").get(1).getLeft().getLeft());
            Assert.assertEquals(1, sessionMap.get("userB").size());

            System.exit(0);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


}
