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
package org.apache.rocketmq.streams.common.monitor.service.impl;

import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.streams.common.monitor.model.JobStage;
import org.apache.rocketmq.streams.common.monitor.model.TraceIdsDO;
import org.apache.rocketmq.streams.common.monitor.model.TraceMonitorDO;
import org.apache.rocketmq.streams.common.monitor.service.MonitorDataSyncService;

public class DBMonitorDataSyncImpl implements MonitorDataSyncService {
    @Override public List<TraceIdsDO> getTraceIds() {
        return null;
    }

    @Override public void updateJobStage(Collection<JobStage> jobStages) {

    }

    @Override public void addTraceMonitor(TraceMonitorDO traceMonitorDO) {

    }
//    @Override
//    public List<TraceIdsDO> getTraceIds() {
//
//        List<TraceIdsDO> traceIdsDOS = ORMUtil.queryForList("SELECT * FROM dipper_trace_ids WHERE now() < gmt_expire", null, TraceIdsDO.class);
////        ORMUtil.querySQL("SELECT * FROM dipper_trace_ids WHERE now() < gmt_expire", TraceIdsDO.class);
//        return traceIdsDOS;
//    }
//
//    @Override
//    public void updateJobStage(JobStage jobStage) {
//        ORMUtil.executeSQL("update dipper_job_stage set input = input+#{input},prev_input = prev_input+#{prevInput}," +
//            "output = output+#{output},last_input_msg=#{lastInputMsg},last_input_msg_time=#{lastInputMsgTime}," +
//            "last_output_msg_time=#{lastOutputMsgTime} where stage_name = #{stageName}", jobStage);
//    }
//
//    @Override
//    public void addTraceMonitor(TraceMonitorDO traceMonitorDO) {
//        ORMUtil.executeSQL("insert into dipper_trace_monitor(trace_id,stage_name,input_number,input_last_msg," +
//            "output_number,output_last_msg,last_input_msg_time,last_output_msg_time) " +
//            "values(#{traceId},#{stageName},#{inputNumber},#{inputLastMsg},#{outputNumber},#{outputLastMsg}," +
//            "#{lastInputMsgTime},#{lastOutputMsgTime}) ON DUPLICATE KEY UPDATE input_number = input_number+#{inputNumber}," +
//            "output_number = output_number+#{outputNumber},input_last_msg=#{inputLastMsg},output_last_msg=#{outputLastMsg}," +
//            "last_input_msg_time=#{lastInputMsgTime},last_output_msg_time=#{lastOutputMsgTime}",traceMonitorDO);
//
//    }
}
