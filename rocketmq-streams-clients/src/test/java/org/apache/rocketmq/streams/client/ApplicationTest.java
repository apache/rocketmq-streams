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

import java.util.List;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.junit.Test;

public class ApplicationTest {

    @Test public void testApplication() throws InterruptedException {


        ConfigurableComponent component=ComponentCreator.getComponent("cloud-siem-collector",ConfigurableComponent.class);
        List<IConfigurable> pipelineList=component.queryConfigurable(Pipeline.TYPE);
        System.out.println(pipelineList.size());
        Thread.sleep(10000000000l);
        //        ComponentCreator.getProperties().put(ConfigureFileKey.POLLING_TIME, "5");
//        ComponentCreator.getProperties().put(ConfigureFileKey.CONNECT_TYPE, "DB");
//        ComponentCreator.getProperties().put("dipper.rds.jdbc.url", "jdbc:mysql://host:port/database?serverTimezone=Asia/Shanghai");
//        ComponentCreator.getProperties().put("dipper.rds.jdbc.username", "username");
//        ComponentCreator.getProperties().put("dipper.rds.jdbc.password", "password");
//
//        ConfigurableComponent configurableComponent = ComponentCreator.getComponent("chris_tmp", ConfigurableComponent.class);
//        StreamTask streamsTask = configurableComponent.queryConfigurable(StreamTask.TYPE, "task");
////        streamsTask.setNameSpace("chris_tmp");
////        streamsTask.setConfigureName("task");
//
//        if (streamsTask != null) {
//            StreamTask copy = new StreamTask();
//            copy.toObject(streamsTask.toJson());
//
//            copy.setUpdateFlag(copy.getUpdateFlag() + 1);
//            configurableComponent.insert(copy);
//            StreamTask streamsTask1 = configurableComponent.queryConfigurable(StreamTask.TYPE, "task");
//            System.out.println(streamsTask1.getUpdateFlag() == copy.getUpdateFlag());
//        }
//        System.out.println(streamsTask.getUpdateFlag());
//        //    configurableComponent.refreshConfigurable("chris_tmp");
//        // System.out.println(streamsTask.getUpdateFlag());
//        while (true) {
//            Thread.sleep(1000);
//
//        }
    }
}
