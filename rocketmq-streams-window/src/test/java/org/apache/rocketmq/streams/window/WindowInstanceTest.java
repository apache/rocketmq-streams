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
package org.apache.rocketmq.streams.window;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.window.model.FireMode;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.junit.Assert;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class WindowInstanceTest {


    @Test
    public void testWindowInstanceNormalMode() {
        WindowOperator window = new WindowOperator();
        window.init();
//        window.doProcessAfterRefreshConfigurable(null);
        window.setFireMode(0);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(5);
        window.setEmitAfterValue(60L);
        window.setNameSpace("namespace_chris");
        window.setConfigureName("name");
        JSONObject msg = new JSONObject();
        msg.put("time", "2021-07-09 11:00:01");
        List<WindowInstance> windowInstances = window.queryOrCreateWindowInstance(new Message(msg), "1");
        assertTrue(windowInstances.size() == 0);
    }

    @Test
    public void testWindowInstanceMode1() {
        WindowOperator window = new WindowOperator();
        window.setFireMode(1);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(1000);
        window.setNameSpace("namespace_chris");
        window.setConfigureName("name");
        JSONObject msg = new JSONObject();
        msg.put("time", "2021-07-09 11:00:01");
        List<WindowInstance> windowInstances = window.queryOrCreateWindowInstance(new Message(msg), "1");
        assertTrue(windowInstances.size() == 1);
        WindowInstance windowInstance = windowInstances.get(0);
        assertTrue(windowInstance.getWindowInstanceName().equals(windowInstance.getFireTime()));
        assertTrue(windowInstance.getStartTime().equals("2021-07-09 11:00:00"));
        assertTrue(windowInstance.getEndTime().equals("2021-07-09 11:05:00"));
        assertTrue(windowInstance.getFireTime().compareTo(DateUtil.getCurrentTimeString()) > 0);
    }

    @Test
    public void testWindowInstanceMode1ExceedWaterMark() {
        WindowOperator window = new WindowOperator();
        window.setFireMode(1);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(5);
        window.setNameSpace("namespace_chris");
        window.setConfigureName("name");
        JSONObject msg = new JSONObject();
        msg.put("time", "2021-07-09 11:00:01");
        List<WindowInstance> windowInstances = window.queryOrCreateWindowInstance(new Message(msg), "1");
        assertTrue(windowInstances.size() == 0);

    }

    @Test
    public void testWindowInstanceMode2() {
        WindowOperator window = new WindowOperator();
        window.setFireMode(2);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(1000);
        window.setNameSpace("namespace_chris");
        window.setConfigureName("name");
        JSONObject msg = new JSONObject();
        msg.put("time", "2021-07-09 11:00:01");
        List<WindowInstance> windowInstances = window.queryOrCreateWindowInstance(new Message(msg), "1");
        assertTrue(windowInstances.size() == 1);
        WindowInstance windowInstance = windowInstances.get(0);
        assertTrue(windowInstance.getWindowInstanceName().equals(windowInstance.getWindowName()));
        assertTrue(windowInstance.getStartTime().equals("2021-07-09 11:00:00"));
        assertTrue(windowInstance.getEndTime().equals("2021-07-09 11:05:00"));
        assertTrue(windowInstance.getSplitId().equals("1"));
        assertTrue(windowInstance.getFireTime().compareTo(DateUtil.getCurrentTimeString()) > 0);
    }

    public List<WindowInstance> hit(String shardId, AbstractWindow window, Long occurTime, Long sysTime,
        int timeUnitAdjust) {
        List<WindowInstance> instanceList = new ArrayList<>();
        int slideInterval = window.getSlideInterval();
        int sizeInterval = window.getSizeInterval();
        if (slideInterval == 0) {
            slideInterval = sizeInterval;
        }
        int maxLateness = window.getWaterMarkMinute();
        List<Date> windowBeginTimeList = DateUtil.getWindowBeginTime(occurTime, slideInterval * timeUnitAdjust * 1000,
            sizeInterval * timeUnitAdjust * 1000);
        for (Date begin : windowBeginTimeList) {
            Date end = DateUtil.addDate(TimeUnit.SECONDS, begin, sizeInterval * timeUnitAdjust);
            Date fire = DateUtil.addDate(TimeUnit.SECONDS, end, maxLateness * timeUnitAdjust);
            Date actualFireDate = fire;
            if (window.getFireMode() != 0) {
                if (sysTime == null || sysTime - end.getTime() < 0) {
                    actualFireDate = end;
                } else {
                    List<Date> currentWindowList = DateUtil.getWindowBeginTime(
                        sysTime, slideInterval * timeUnitAdjust * 1000,
                        sizeInterval * timeUnitAdjust * 1000);
                    if (!CollectionUtil.isEmpty(currentWindowList)) {
                        Date soonBegin = currentWindowList.get(currentWindowList.size() - 1);
                        Date soonEnd = DateUtil.addDate(TimeUnit.SECONDS, soonBegin,
                            sizeInterval * timeUnitAdjust);
                        Date soonFire = soonEnd;
                        Date recentFireTime = DateUtil.addDate(TimeUnit.SECONDS, soonFire, -(sizeInterval * timeUnitAdjust));
                        Date stopTime = DateUtil.addDate(TimeUnit.SECONDS, end, maxLateness * timeUnitAdjust * 1000);
                        //窗口结束时间处于窗口中间位置
                        if (recentFireTime.compareTo(stopTime) < 0 && stopTime.compareTo(soonFire) <= 0) {
                            actualFireDate = stopTime;
                        } else {
                            actualFireDate = soonFire;
                        }
                    }
                    if (actualFireDate.getTime() - end.getTime() - maxLateness * timeUnitAdjust * 1000 > 0) {
                        System.err.println("window instance out of date!!! " + DateUtil.format(begin) + "-" + DateUtil.format(end) + "---" + DateUtil.format(fire));
                        break;
                    }
                }
            } else {
                if (sysTime != null && sysTime - actualFireDate.getTime() > 0) {
                    System.err.println("window instance out of date!!! " + DateUtil.format(begin) + "-" + DateUtil.format(end) + "---" + DateUtil.format(fire));
                    break;
                }
            }
            String startTime = DateUtil.format(begin);
            String endTime = DateUtil.format(end);
            String fireTime = DateUtil.format(fire);
            String actualFireTime = DateUtil.format(actualFireDate);
            //keep the state util fire time when fire mode == 2
            WindowInstance instance = (FireMode.ACCUMULATED.equals(FireMode.valueOf(window.getFireMode()))) ? window.createWindowInstance(startTime, endTime, fireTime, shardId) : window.createWindowInstance(startTime, endTime, actualFireTime, shardId);
//            instance.setActualFireTime(actualFireTime);
            instanceList.add(instance);
        }
        return instanceList;
    }

    @Test
    @Deprecated
    public void testHitWindowInstance() {
        //NORMAL
        WindowOperator window = new WindowOperator();
        window.setFireMode(0);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(0);
        window.setNameSpace("namespace_test");
        window.setConfigureName("window_name");

        JSONObject msg = new JSONObject();
        String eventTime = "2021-08-27 18:03:00";
        msg.put("time", eventTime);

        String shardId = "000";
        String currentTime = "2021-08-27 18:04:00";
        Long occurTimeStamp = DateUtil.parseTime(eventTime).getTime();
        Long currentTimeStamp = DateUtil.parseTime(currentTime).getTime();

        List<WindowInstance> hitList = hit(shardId, window, occurTimeStamp, currentTimeStamp, 60);
        Assert.assertEquals(1, hitList.size());
        Assert.assertEquals("2021-08-27 18:00:00", hitList.get(0).getStartTime());
        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getFireTime());
//        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getActualFireTime());

        String anotherEventTime = "2021-08-27 18:04:00";
        Long anotherEventTimeStamp = DateUtil.parseTime(anotherEventTime).getTime();
        hitList = hit(shardId, window, anotherEventTimeStamp, currentTimeStamp, 60);
        Assert.assertEquals(1, hitList.size());
        Assert.assertEquals("2021-08-27 18:00:00", hitList.get(0).getStartTime());
        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getFireTime());
//        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getActualFireTime());

        //PARTITIONED
        window = new WindowOperator();
        window.setFireMode(1);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(120);
        window.setNameSpace("namespace_test");
        window.setConfigureName("window_name");

        msg = new JSONObject();
        eventTime = "2021-08-27 17:03:00";
        msg.put("time", eventTime);

        currentTime = "2021-08-27 18:00:00";
        occurTimeStamp = DateUtil.parseTime(eventTime).getTime();
        currentTimeStamp = DateUtil.parseTime(currentTime).getTime();

        hitList = hit(shardId, window, occurTimeStamp, currentTimeStamp, 60);
        Assert.assertEquals(1, hitList.size());
        Assert.assertEquals("2021-08-27 17:00:00", hitList.get(0).getStartTime());
        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getFireTime());
//        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getActualFireTime());

        anotherEventTime = "2021-08-27 17:04:00";
        anotherEventTimeStamp = DateUtil.parseTime(anotherEventTime).getTime();
        hitList = hit(shardId, window, anotherEventTimeStamp, currentTimeStamp, 60);
        Assert.assertEquals(1, hitList.size());
        Assert.assertEquals("2021-08-27 17:00:00", hitList.get(0).getStartTime());
        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getFireTime());
//        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getActualFireTime());

        //ACCUMULATED
        window = new WindowOperator();
        window.setFireMode(1);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(120);
        window.setNameSpace("namespace_test");
        window.setConfigureName("window_name");

        msg = new JSONObject();
        eventTime = "2021-08-27 17:03:00";
        msg.put("time", eventTime);

        currentTime = "2021-08-27 18:00:00";
        occurTimeStamp = DateUtil.parseTime(eventTime).getTime();
        currentTimeStamp = DateUtil.parseTime(currentTime).getTime();

        hitList = hit(shardId, window, occurTimeStamp, currentTimeStamp, 60);
        Assert.assertEquals(1, hitList.size());
        Assert.assertEquals("2021-08-27 17:00:00", hitList.get(0).getStartTime());
        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getFireTime());
//        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getActualFireTime());

        anotherEventTime = "2021-08-27 17:04:00";
        anotherEventTimeStamp = DateUtil.parseTime(anotherEventTime).getTime();
        hitList = hit(shardId, window, anotherEventTimeStamp, currentTimeStamp, 60);
        Assert.assertEquals(1, hitList.size());
        Assert.assertEquals("2021-08-27 17:00:00", hitList.get(0).getStartTime());
        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getFireTime());
//        Assert.assertEquals("2021-08-27 18:05:00", hitList.get(0).getActualFireTime());
    }
}
