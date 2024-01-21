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
package org.apache.rocketmq.streams.huawei;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.junit.Before;
import org.junit.Test;
import org.quartz.CronScheduleBuilder;
import org.quartz.Job;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.SchedulerFactory;
import org.quartz.Trigger;
import org.quartz.TriggerBuilder;
import org.quartz.impl.StdSchedulerFactory;

public class TimeTest {

    private String startTime = "2021-07-12 12:13:10";

    private Integer cycle = 3;

    private String cycleUnit = "hours";

    private long cycleTime = 0L;

    @Before
    public void init() {
        if (cycleUnit == null || cycleUnit.isEmpty()) {
            cycleUnit = TimeUnit.SECONDS.toString();
        }
        if (cycleUnit.equalsIgnoreCase(TimeUnit.DAYS.toString())) {
            cycleTime = cycle * 24 * 60 * 60 * 1000;
        } else if (cycleUnit.equalsIgnoreCase(TimeUnit.HOURS.toString())) {
            cycleTime = cycle * 60 * 60 * 1000;
        } else if (cycleUnit.equalsIgnoreCase(TimeUnit.MINUTES.toString())) {
            cycleTime = cycle * 60 * 1000;
        } else {
            if (cycleUnit.equalsIgnoreCase(TimeUnit.SECONDS.toString())) {
                cycleTime = cycle * 1000;
            }
        }
    }

    @Test
    public void test3() {
        long start = DateUtil.parse("2021-07-11 11:59:10").getTime();
        long current = DateUtil.parse("2021-07-13 00:01:10").getTime();
        Long offset = DateUtil.getWindowStartWithOffset(start, 0, 24 * 60 * 60 * 1000);
        System.out.println(new Date(offset));
    }

    @Test
    public void test() {
        long start = DateUtil.parse("2021-07-14 11:59:10").getTime();
        long current = DateUtil.parse("2021-07-14 08:01:10").getTime() - 8 * 5 * 60 * 1000;

        List<Date> dateList = DateUtil.getWindowBeginTime(current, 5 * 60 * 1000, current - start);

        for (Date date : dateList) {
            System.out.println(DateUtil.format(date));
        }
    }

    @Test
    public void test1() throws SchedulerException {
        SchedulerFactory factory = new StdSchedulerFactory();
        Scheduler scheduler = factory.getScheduler();
        scheduler.start();
        // 2.创建JobDetail实例，并与MyJob类绑定(Job执行内容)
        JobDetail job = JobBuilder.newJob(MyJob.class)
            .withIdentity("job1", "group1")
            .build();

        // 3.构建Trigger实例,每隔30s执行一次
        Trigger trigger = TriggerBuilder.newTrigger()
            .withIdentity("trigger1", "group1")
            .startNow()
            .withSchedule(CronScheduleBuilder.cronSchedule("0/5 * * * * ?"))
            .build();
        scheduler.scheduleJob(job, trigger);

        while (true) {
            try {
                Thread.sleep(60000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

    }

    @Test
    public void test2() {
        String path = "LTS-test/%Y/%m/%done/%H/%m";
        System.out.println(path.replaceAll("%Y", "2023").replaceAll("%M", ""));

    }

    public static class MyJob implements Job {

        private Long startTime = DateUtil.parseTime("2023-07-19 01:00:03").getTime();
        private Long cycle = 5 * 1000L; // 5分钟

        @Override
        public void execute(JobExecutionContext context) throws JobExecutionException {
            long currentTime = System.currentTimeMillis();

            for (startTime = (startTime % cycle == 0 ? startTime : startTime + (cycle - startTime % cycle)); startTime < currentTime - cycle; startTime = startTime + cycle) {
                System.out.println(DateUtil.format(new Date(currentTime), "yyyy-MM-dd HH:mm:ss:SSS") + "-------" + DateUtil.format(new Date(startTime), "yyyy-MM-dd HH:mm:ss:SSS"));
            }

        }

        public Long getStartTime() {
            return startTime;
        }

        public void setStartTime(Long startTime) {
            this.startTime = startTime;
        }

        public Long getCycle() {
            return cycle;
        }

        public void setCycle(Long cycle) {
            this.cycle = cycle;
        }
    }

}
