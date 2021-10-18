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

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.utils.ThreadUtil;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @description
 */
public class ScheduledStreamBuilder {

    static final Log logger = LogFactory.getLog(ScheduledStreamBuilder.class);

    protected ScheduledExecutorService balanceExecutor;

    TimeUnit timeUnit;

    int interval;

    ScheduledTask task;



    public ScheduledStreamBuilder(int interval, TimeUnit timeUnit){
        this.interval = interval;
        this.timeUnit = timeUnit;
        balanceExecutor = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().namingPattern("cycle-builder-task-%d").daemon(true).build());
    }

    public void setTask(ScheduledTask task) {
        this.task = task;
    }

    public void start(){
        balanceExecutor.scheduleAtFixedRate(task, 0, interval, timeUnit);

        while(true){
            Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
            for(Thread th : threadSet){
                logger.error(String.format("CycleStreamBuilder size %d, name is %s, stack is %s. ", threadSet.size(), th.getName(), Arrays.toString(th.getStackTrace())));
            }

            ThreadUtil.sleep(10000);
        }
    }
}
