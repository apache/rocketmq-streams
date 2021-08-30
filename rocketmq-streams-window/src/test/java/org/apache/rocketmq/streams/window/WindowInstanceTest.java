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
import java.util.List;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class WindowInstanceTest {

    /**
     * save window instance
     */
    @Test
    public void testWindowInstanceSave() {
        WindowOperator window = new WindowOperator();
        window.setNameSpace("namespace_chris");
        window.setConfigureName("name");
        WindowInstance windowInstance = window.createWindowInstance("2021-07-09 11:00:00", "2021-07-09 11:10:00", "2021-07-09 11:10:00", "1");
        ORMUtil.batchReplaceInto(windowInstance);
        WindowInstance queryWindowInstance = ORMUtil.queryForObject("select * from window_instance where window_instance_key='" + windowInstance.getWindowInstanceKey() + "'", null, WindowInstance.class);
        assertTrue(queryWindowInstance != null);
    }

    @Test
    public void testWindowInstanceNormalMode() {
        WindowOperator window = new WindowOperator();
        window.setFireMode(0);
        window.setTimeFieldName("time");
        window.setSlideInterval(5);
        window.setSizeInterval(5);
        window.setWaterMarkMinute(0);
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
}
