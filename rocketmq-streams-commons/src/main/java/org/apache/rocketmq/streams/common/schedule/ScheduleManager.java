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
package org.apache.rocketmq.streams.common.schedule;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;

public class ScheduleManager {
    protected List<ScheduleTask> scheduleTasks=new ArrayList<>();
    protected AtomicBoolean isStart=new AtomicBoolean(false);
    protected ScheduledExecutorService scheduledExecutorService= new ScheduledThreadPoolExecutor(5);
    protected ExecutorService executorService= ThreadPoolFactory.createThreadPool(2,50);
    private static ScheduleManager scheduleManager=new ScheduleManager();
    public static ScheduleManager getInstance(){
        return scheduleManager;
    }


    public void regist(ScheduleTask scheduleTask){
        start();
        if(scheduleTask==null){
            return;
        }
        scheduleTasks.add(scheduleTask);
    }


    public void start(){
        if(isStart.compareAndSet(false,true)){
            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override public void run() {
                    try {
                        List<ScheduleTask> list=new ArrayList<>(scheduleTasks);
                        for(ScheduleTask scheduleTask:list){
                            if(scheduleTask!=null&&scheduleTask.canExecute()){
                                executeTask(scheduleTask);
                            }
                        }
                    }catch (Exception e){
                        e.printStackTrace();
                    }

                }
            },0,100, TimeUnit.MILLISECONDS);
        }
    }

    protected void executeTask(ScheduleTask task) {
        if(task.getExecutorService()!=null){
            task.getExecutorService().execute(task.getRunnable());
        }else {
            executorService.execute(task.getRunnable());
        }
    }
}
