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
package org.apache.rocketmq.streams.schedule.job;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.streams.common.interfaces.IScheduleExecutor;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.monitor.MonitorFactory;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.StatefulJob;

@DisallowConcurrentExecution
public class ConfigurableExecutorJob implements StatefulJob {
    private Log LOG = LogFactory.getLog(ConfigurableExecutorJob.class);

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        IScheduleExecutor channelExecutor = null;

        try {
            JobDetail jobDetail = context.getJobDetail();
            channelExecutor = (IScheduleExecutor)jobDetail.getJobDataMap().get(IScheduleExecutor.class.getName());
            channelExecutor.doExecute();
        } catch (Exception e) {
            //降低日志量
            //            LOG.error("schedule error "+channelExecutor.toString(),e);
            IMonitor startupMonitor = MonitorFactory.getOrCreateMonitor(MapKeyUtil.createKey(MonitorFactory.PIPLINE_START_UP, channelExecutor.getNameSpace()));
            IMonitor monitor = startupMonitor.createChildren(channelExecutor.getConfigureName());
            monitor.addContextMessage(JSON.parse(channelExecutor.toString()));
            String name = MapKeyUtil.createKeyBySign(".", channelExecutor.getNameSpace(), channelExecutor.getConfigureName());
            monitor.occureError(e, name + " schedule error", e.getMessage());
        }

    }
}
