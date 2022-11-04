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
package org.apache.rocketmq.streams.dispatcher;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.dispatcher.cache.RocketmqCache;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.impl.RocketmqDispatcher;
import org.junit.Test;

public class DispatcherTest {
    private transient RocketmqDispatcher<?> dispatcher;
    private transient IDispatcherCallback balanceCallback;
    private String filePath="/tmp/dipper/task.txt";
    @Test
    public void testDispatcher() throws InterruptedException {
        ComponentCreator.getProperties().put("dipper.dispatcher.rocketmq.nameserv","11.166.49.226:9876");
        try {
            if (this.balanceCallback == null) {
                this.balanceCallback = new IDispatcherCallback() {

                    @Override public List<String> start(List<String> names) {
                        if(CollectionUtil.isEmpty(names)){
                            return names;
                        }
                        for(String name:names){
                            System.out.println("start task "+name+" "+ DateUtil.getCurrentTimeString());
                        }
                        return names;
                    }

                    @Override public List<String> stop(List<String> names) {
                        if(CollectionUtil.isEmpty(names)){
                            return names;
                        }
                        for(String name:names){
                            System.out.println("stop task "+name+" "+ DateUtil.getCurrentTimeString());
                        }
                        return names;
                    }

                    @Override public List<String> list(List<String> instanceIdList) {
                        List<String> names= FileUtil.loadFileLine(filePath);
                        System.out.println("load task count is "+names.size()+" "+ DateUtil.getCurrentTimeString());
                        return names;
                    }
                };
            }
            if (this.dispatcher == null) {
                String dispatcherType = ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_TYPE, "rocketmq");
                if (dispatcherType.equalsIgnoreCase("rocketmq")) {
                    String nameServAddr = ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_ROCKETMQ_NAMESERV, "127.0.0.1:9876");
                    String voteTopic = ComponentCreator.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_ROCKETMQ_TOPIC, "VOTE_TOPIC");
                    this.dispatcher = new RocketmqDispatcher(nameServAddr, voteTopic, IdUtil.instanceId(), "dipper", DispatchMode.AVERAGELY, this.balanceCallback, new RocketmqCache(nameServAddr));
                }
            }
            this.dispatcher.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        Thread.sleep(1000000000l);
    }


    @Test
    public void testTask(){
        List<String> tasks=new ArrayList<>();
        for(int i=0;i<10;i++){
            tasks.add("task_"+i);
        }

        tasks.remove("task_"+11);
        tasks.remove("task_"+12);
        FileUtil.write(filePath,tasks);
    }


}
