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
package org.apache.rocketmq.streams.schedule.service.impl;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.interfaces.IScheduleExecutor;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.schedule.job.ConfigurableExecutorJob;
import org.apache.rocketmq.streams.schedule.service.IScheduleService;
import org.quartz.JobBuilder;
import org.quartz.JobDataMap;
import org.quartz.JobDetail;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.TriggerBuilder.newTrigger;

public class ScheduleServiceImpl implements IScheduleService {
    protected Scheduler scheduler;

    public ScheduleServiceImpl() {
        try {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
        } catch (SchedulerException e) {
            throw new RuntimeException("create scheduler container error ", e);
        }
    }

    @Override
    public void startSchedule(IScheduleExecutor channelExecutor, String cronTable, boolean startNow) {
        String name = MapKeyUtil.createKey(channelExecutor.getType(), channelExecutor.getConfigureName());
        Trigger trigger = newTrigger()
            .withIdentity(name, channelExecutor.getNameSpace())
            .withSchedule(cronSchedule(cronTable))
            .forJob(name, channelExecutor.getNameSpace())
            .build();
        try {
            JobDetail jobDetail = createJobDetail(channelExecutor);
            scheduler.scheduleJob(jobDetail, trigger);
            if (startNow) {
                Trigger startNowTrigger = newTrigger()
                    .withIdentity(name + "_now", channelExecutor.getNameSpace())
                    .forJob(jobDetail)
                    .startNow()
                    .build();

                scheduler.scheduleJob(startNowTrigger);
            }

        } catch (SchedulerException e) {
            throw new RuntimeException("create schedule erro " + channelExecutor.toString(), e);
        }
    }

    @Override
    public void startSchedule(IScheduleExecutor receiver, Date date) {
        int second = DateUtil.getSecond(date);
        int mintue = DateUtil.getMinute(date);
        int hour = DateUtil.getHour(date);
        int day = DateUtil.getDay(date);
        int month = DateUtil.getMonth(date);
        String year = DateUtil.getYear(date);
        String cron = second + " " + mintue + " " + hour + " " + day + " " + month + " ? " + year;
        startSchedule(receiver, cron, false);
    }

    @Override
    public void startSchedule(IScheduleExecutor receiver, int secondPollingTime, boolean startNow) {
        String cron = convertCron(secondPollingTime);
        startSchedule(receiver, cron, startNow);
    }

    @Override
    public String convertCron(int secondPollingTime) {
        String cron = null;
        if (secondPollingTime < 60) {
            cron = "0/" + secondPollingTime + " * * * * ?";
        } else if (secondPollingTime < 3600) {
            int minute = secondPollingTime / 60;
            int second = secondPollingTime % 60;
            cron = second + " 0/" + minute + " * * * ?";
        } else if (secondPollingTime < 86400) {
            int hour = secondPollingTime / 3600;
            int other = secondPollingTime % 3600;
            String minuteStr = "0";
            String secondStr = "0";
            if (other > 60) {
                int minute = other / 60;
                int second = other % 60;
                minuteStr = minute + "";
                secondStr = second + "";
            } else {
                secondStr = other + "";
            }
            cron = secondStr + " " + minuteStr + " " + "0/" + hour + " * * ?";
        } else if (secondPollingTime < 86400 * 2) {
            secondPollingTime = secondPollingTime % 86400;
            int hour = secondPollingTime / 3600;
            int other = secondPollingTime % 3600;
            String minuteStr = "0";
            String secondStr = "0";
            if (other > 60) {
                int minute = other / 60;
                int second = other % 60;
                minuteStr = minute + "";
                secondStr = second + "";
            } else {
                secondStr = other + "";
            }
            cron = secondStr + " " + minuteStr + " " + hour + " * * ?";
        } else {
            throw new RuntimeException("can not support this value ,please use startSchedule cron method");
        }
        return cron;
    }

    @Override
    public void startScheduleUsingPollingTime(IScheduleExecutor channelExecutor, int pollingTime, TimeUnit timeUnit, boolean startNow) {
        String cronTable = createCronTableStr(pollingTime, timeUnit);
        startSchedule(channelExecutor, cronTable, startNow);
    }

    @Override
    public void startScheduleDailyTime(IScheduleExecutor channelExecutor, String dateTime, boolean startNow) {
        String cronTable = createCronTableStr(dateTime, TimeUnit.DAYS);
        startSchedule(channelExecutor, cronTable, startNow);
    }

    @Override
    public void startScheduleHourTime(IScheduleExecutor channelExecutor, String dateTime, boolean startNow) {
        String cronTable = createCronTableStr(dateTime, TimeUnit.HOURS);
        startSchedule(channelExecutor, cronTable, startNow);
    }

    public void start() {
        try {
            this.scheduler.start();
        } catch (SchedulerException e) {
            throw new RuntimeException("start schedule error ", e);
        }
    }

    public void stop() {
        try {
            this.scheduler.shutdown();
        } catch (SchedulerException e) {
            throw new RuntimeException("start schedule error ", e);
        }
    }

    /**
     * 创建jobdetail
     *
     * @param executor
     * @return
     */
    protected JobDetail createJobDetail(IScheduleExecutor executor) {
        JobDataMap jobDataMap = new JobDataMap();
        jobDataMap.put(IScheduleExecutor.class.getName(), executor);
        String name = MapKeyUtil.createKey(executor.getType(), executor.getConfigureName());
        return JobBuilder.newJob(ConfigurableExecutorJob.class)
            .withIdentity(name, executor.getNameSpace()) // name "myJob", group "group1"
            .usingJobData(jobDataMap)
            .build();
    }

    /**
     * 轮询时间，多长时间轮询一次
     *
     * @param pollingTime
     * @param timeUnit
     * @return
     */
    protected String createCronTableStr(int pollingTime, TimeUnit timeUnit) {
        String cronTable = null;
        if (pollingTime > 60) {
            throw new RuntimeException("pollingTime can not exceed 60, must in the unit " + timeUnit + ". the value is " + pollingTime);
        }
        if (TimeUnit.DAYS == timeUnit && pollingTime > 31) {
            throw new RuntimeException("pollingTime can not exceed 31, must in the day unit . the value is " + pollingTime);
        }
        if (TimeUnit.SECONDS == timeUnit) {
            cronTable = "0/" + pollingTime + " * * * * ?";
        } else if (TimeUnit.MINUTES == timeUnit) {
            cronTable = "0 0/" + pollingTime + " * * * ?";
        } else if (TimeUnit.HOURS == timeUnit) {
            cronTable = "0 0 0/" + pollingTime + " * * ?";
        } else if (TimeUnit.DAYS == timeUnit) {
            cronTable = "0 0 0 0/" + pollingTime + " * ?";
        } else {
            throw new RuntimeException("can not support the timeunit");
        }
        return cronTable;
    }

    /**
     * 每天／每小时，几点执行一次
     *
     * @param dateTime
     * @param timeUnit
     * @return
     */
    protected String createCronTableStr(String dateTime, TimeUnit timeUnit) {
        String cronTable = null;
        if (TimeUnit.DAYS == timeUnit) {
            String[] values = dateTime.split(":");
            String hour = getTimeValue(values[0]);
            String minute = getTimeValue(values[1]);
            String second = getTimeValue(values[2]);
            cronTable = second + " " + minute + " " + hour + " 0/1 * ?";
        } else if (TimeUnit.HOURS == timeUnit) {
            String[] values = dateTime.split(":");
            String minute = getTimeValue(values[0]);
            String second = getTimeValue(values[1]);
            cronTable = second + " " + minute + " 0/1 * * ?";
        } else {
            throw new RuntimeException("can not support the timeunit");
        }
        return cronTable;
    }

    private String getTimeValue(String value) {
        if (value.startsWith("0")) {
            return value.substring(1);
        }
        return value;
    }

    public static void main(String[] args) throws SchedulerException {
        Date date = new Date();
        int second = DateUtil.getSecond(date);
        int mintue = DateUtil.getMinute(date);
        int hour = DateUtil.getHour(date);
        int day = DateUtil.getDay(date);
        int month = DateUtil.getMonth(date);
        String year = DateUtil.getYear(date);
        String cron = second + " " + mintue + " " + hour + " " + day + " " + month + " ? " + year;
        System.out.println(cron);
    }
}
