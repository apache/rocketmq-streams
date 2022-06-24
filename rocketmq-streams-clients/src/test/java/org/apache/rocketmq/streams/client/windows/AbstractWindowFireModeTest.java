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
package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.functions.ForEachFunction;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.utils.DateUtil;


public abstract class AbstractWindowFireModeTest implements Serializable {
    protected Date date=new Date();
    public AbstractWindowFireModeTest(){

        date.setYear(2021-1900);
        date.setMonth(6);
        date.setDate(14);
        date.setHours(12);
        date.setMinutes(1);
        date.setSeconds(0);

    }

    public void testWindowFireMode0(boolean isLocalOnly) throws InterruptedException {
        testWindowFireMode1(isLocalOnly,5);
    }

    public void  testWindowFireMode0(boolean isLocalOnly,int windowSize) throws InterruptedException {

        createSourceDataStream().map(new MapFunction<JSONObject, String>() {
                int count=0;
                Long time=null;
                @Override
                public JSONObject map(String message) throws Exception {

                    if(time==null){
                        time=date.getTime();
                    }else {
                        time+=count;
                    }
                    count++;
                    JSONObject msg=JSONObject.parseObject(message);

                    msg.put("logTime",time);

                    return msg;
                }
            })
            .window(TumblingWindow.of(Time.seconds(windowSize)))
            .groupBy("ProjectName", "LogStore")
            .setLocalStorageOnly(isLocalOnly)
            .setMaxMsgGap(isLocalOnly?10L:20L)
            .setTimeField("logTime")
            .count("total")
            .sum("OutFlow", "OutFlow")
            .sum("InFlow", "inflow")
            .toDataStream()
            .forEach(new ForEachFunction<JSONObject>() {
                AtomicInteger sum = new AtomicInteger(0) ;
                @Override
                public synchronized void foreach(JSONObject o) {
                    int total = o.getInteger("total");
                    o.put("sum(total)",  sum.addAndGet(total));
                }
            }).toPrint().start();
    }
    public void testWindowFireMode1(boolean isLocalOnly) throws InterruptedException {
        testWindowFireMode1(isLocalOnly,5);
    }
    public void testWindowFireMode1(boolean isLocalOnly,int windowSize) throws InterruptedException {
        AtomicInteger sum = new AtomicInteger(0) ;
            createSourceDataStream()
            //.map(new MapFunction<JSONObject, String>() {
            //    AtomicInteger COUNT=new AtomicInteger(0);
            //    Long time;
            //    @Override
            //    public JSONObject map(String message) throws Exception {
            //
            //        if(time==null){
            //            time=date.getTime();
            //        }else {
            //            int count=COUNT.incrementAndGet();
            //            time+=count;
            //        }
            //        JSONObject msg=JSONObject.parseObject(message);
            //
            //        msg.put("logTime",time);
            //        return msg;
            //    }
            //})
            .window(TumblingWindow.of(Time.seconds(windowSize)))
            .setTimeField("logTime")
            .fireMode(1)
                .setMaxMsgGap(isLocalOnly?20L:20L)
            .waterMark(100000000)
            .groupBy("ProjectName", "LogStore")
            .setLocalStorageOnly(isLocalOnly)
            .count("total")
            .sum("OutFlow", "OutFlow")
            .sum("InFlow", "InFlow")
            .toDataStream()
            .forEach(new ForEachFunction<JSONObject>() {


                @Override
                public synchronized void foreach(JSONObject o) {
                    int total = o.getInteger("total");
                    o.put("sum(total)",  sum.addAndGet(total));
                }
            }).toPrint().start();
    }

    public void testWindowFireMode2(boolean isLocalOnly){
        long time=new Date().getTime();
        System.out.println(DateUtil.getCurrentTimeString());
        createSourceDataStream()
            .map(new MapFunction<JSONObject, String>() {
                int count=0;
                @Override
                public JSONObject map(String message) throws Exception {

                    JSONObject msg=JSONObject.parseObject(message);
                    long time= msg.getLong("logTime");
                    Date date=new Date(time);
                    date.setYear(2021-1900);
                    date.setMonth(6);
                    date.setDate(14);
                    msg.put("logTime",date.getTime()+count++);
                    return msg;
                }
            })
            .window(TumblingWindow.of(Time.seconds(5)))
            .setTimeField("logTime")
            .setMaxMsgGap(isLocalOnly?5L:20L)
            .fireMode(1)
            .waterMark(100000000)
            .groupBy("ProjectName", "LogStore")
            .setLocalStorageOnly(isLocalOnly)
            .count("total")
            .sum("OutFlow", "OutFlow")
            .sum("InFlow", "InFlow")
            .toDataStream()
            .map(new MapFunction<JSONObject, JSONObject>() {
                long time=new Date().getTime();
                @Override
                public JSONObject map(JSONObject message) throws Exception {
                    message.put("name","chris");
                    message.put("time",time++);
                    return message;
                }
            })
            .window(TumblingWindow.of(Time.seconds(5)))
            .fireMode(2).waterMark(100000000)
            .setMaxMsgGap(80L)
            .groupBy("name")
            .setTimeField("time")
            .sum("total","sum_total")
            .setLocalStorageOnly(true)
            .toDataStream()
            .forEach(new ForEachFunction<JSONObject>() {
                AtomicInteger sum = new AtomicInteger(0) ;
                Map<String,Integer> map=new HashMap<>();
                @Override
                public synchronized void foreach(JSONObject o) {
                    String windowInstanceId=o.getString("windowInstanceId");
                    Integer oldValue=map.get(windowInstanceId);
                    int total = o.getInteger("sum_total");
                    if(oldValue!=null){
                        total=total-oldValue;
                    }
                    int nowValue=sum.addAndGet(total);
                    map.put(windowInstanceId,total);
                    o.put("sum(total)",  nowValue);
                }
            }).toPrint().start();

    }


    protected abstract DataStream createSourceDataStream();
}
