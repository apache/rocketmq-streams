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
package org.apache.rocketmq.streams.schedule.service;

import org.apache.rocketmq.streams.common.interfaces.IScheduleExecutor;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 基于quartz 封装定时调度组件
 */
public interface IScheduleService {

    /**
     * 启动一个调度，
     *
     * @param executor  到时间后，需要执行的业务逻辑
     * @param crotabStr 基于cron语法的定时描述
     * @param startNow  是否立刻执行一次
     */
    void startSchedule(IScheduleExecutor executor, String crotabStr, boolean startNow);

    /**
     * 在某个具体时间点执行
     *
     * @param executor 到时间后，需要执行的业务逻辑
     * @param date     具体的执行时间
     */
    void startSchedule(IScheduleExecutor executor, Date date);

    /**
     * n秒轮询一次，允许超过60，需要转化成对应的小时，分，天
     *
     * @param executor          到时间后，需要执行的业务逻辑
     * @param secondPollingTime 轮询时间，单位是秒
     * @param startNow          是否立刻执行一次
     */
    void startSchedule(IScheduleExecutor executor, int secondPollingTime, boolean startNow);

    /**
     * n秒轮询一次，允许超过60，需要转化成对应的小时，分，天
     *
     * @param executor    到时间后，需要执行的业务逻辑
     * @param pollingTime 轮询时间
     * @param timeUnit    轮询时间的单位
     * @param startNow    是否立刻执行一次
     */
    void startScheduleUsingPollingTime(IScheduleExecutor executor, int pollingTime, TimeUnit timeUnit, boolean startNow);

    /**
     * 每天具体时间跑一次
     *
     * @param executor 到时间后，需要执行的业务逻辑
     * @param dateTime 15:00:00
     * @param startNow 是否立刻执行一次
     */
    void startScheduleDailyTime(IScheduleExecutor executor, String dateTime, boolean startNow);

    /**
     * 每小时跑一次，指定开始的分钟
     *
     * @param executor 到时间后，需要执行的业务逻辑
     * @param dateTime 15:00
     * @param startNow 是否立刻执行一次
     */
    void startScheduleHourTime(IScheduleExecutor executor, String dateTime, boolean startNow);

    /**
     * 把轮询时间转化成cron
     *
     * @param secondPollingTime 轮询时间，单位是秒
     * @return cron时间描述
     */
    String convertCron(int secondPollingTime);
}
