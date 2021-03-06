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
package org.apache.rocketmq.streams.common.monitor;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.monitor.model.JobStage;
import org.apache.rocketmq.streams.common.monitor.model.TraceIdsDO;
import org.apache.rocketmq.streams.common.monitor.model.TraceMonitorDO;
import org.apache.rocketmq.streams.common.monitor.service.MonitorDataSyncService;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;

public class ConsoleMonitorManager {

    private static final Log LOG = LogFactory.getLog(ConsoleMonitorManager.class);

    public static final int MSG_FILTERED = -1;
    public static final int MSG_NOT_FLOWED = 0;
    public static final int MSG_FLOWED = 1;

    private static ConsoleMonitorManager monitorManager = new ConsoleMonitorManager();
    private Map<String, JobStage> cache = new ConcurrentHashMap();
    private Map<String, TraceMonitorDO> traceCache = new ConcurrentHashMap();
    private Set<String> validTraceIds = new HashSet<String>();
    private MonitorDataSyncService monitorDataSyncService = MonitorDataSyncServiceFactory.create();

    public static ConsoleMonitorManager getInstance() {
        return monitorManager;
    }

    /**
     * ???????????? static ??????????????????????????? ConsoleMonitorManager ???????????????????????????????????????????????????????????????
     * ??????????????????????????????????????????????????????????????????????????????????????????????????????
     * 1. ???????????????????????? traceId ????????????
     * 2. ?????? dipper_job_stage ???
     * 3. ?????? dipper_trace_monitor ???
     */
    public ConsoleMonitorManager() {
        if (!isConsoleOpen()) {
            return;
        }

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    queryValidTraceIds();
                    Map<String, JobStage> jobStageMap = cache;
                    cache = new ConcurrentHashMap();
                    long current = System.currentTimeMillis();
                    for (JobStage jobStage : jobStageMap.values()) {
                        jobStage.setMachineName("");
//                        jobStage.setLastInputMsgObj(new JSONObject());
//                        String msg = msgObj.toJSONString();
//                        if (msg != null && !"".equalsIgnoreCase(msg)){
//                            jobStage.setLastInputMsg(msg);
//                        }
                        jobStage.setInput(jobStage.getSafeInput().getAndSet(0));
                        jobStage.setOutput(jobStage.getSafeOutput().getAndSet(0));

                        double tps = jobStage.getInput() / ((current - jobStage.getCreateTime()) * 1.0 / 1000);
                        jobStage.setTps((double) (Math.round(tps * 100)) / 100);

                    }
                    monitorDataSyncService.updateJobStage(jobStageMap.values());
//                    for (JobStage jobStage : jobStageMap.values()) {
//                        jobStage.setMachineName("");
//                        String msg = JSON.toJSONString(jobStage.getLastInputMsgObj());
//                        if (msg != null && msg.length() > 2){
//                            jobStage.setLastInputMsg(msg);
//                        }
//                        jobStage.setInput(jobStage.getSafeInput().getAndSet(0));
//                        jobStage.setOutput(jobStage.getSafeOutput().getAndSet(0));
//                        if(jobStage.getInput() != 0 || jobStage.getOutput() != 0 || jobStage.getPrevInput() != 0){
//                            monitorDataSyncService.updateJobStage(jobStage);
////                            DBManager.executeSQL("update dipper_job_stage set input = input+#{input},prev_input = prev_input+#{prevInput},output = output+#{output},last_input_msg=#{lastInputMsg},last_input_msg_time=#{lastInputMsgTime},last_output_msg_time=#{lastOutputMsgTime} where stage_name = #{stageName}",jobStage);
//                        }
//                        jobStage.setPrevInput(jobStage.getInput());
//                    }
                    if (validTraceIds.size() > 0) {
                        for (TraceMonitorDO traceMonitorDO : traceCache.values()) {
                            traceMonitorDO.setInputNumber(traceMonitorDO.getSafeInput().getAndSet(0));
                            traceMonitorDO.setOutputNumber(traceMonitorDO.getSafeOutput().getAndSet(0));
                            if (traceMonitorDO.getInputNumber() != 0 || traceMonitorDO.getOutputNumber() != 0) {
                                monitorDataSyncService.addTraceMonitor(traceMonitorDO);
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("ConsoleMonitorManager report error!", e);
                }
            }
        }, 20, 30, TimeUnit.SECONDS);
    }

    public Set<String> getValidTraceIds() {
        return validTraceIds;
    }

    /**
     * traceId ??????
     */
    public void reportChannel(ChainPipeline pipeline, ISource source, IMessage message) {
        if (!isConsoleOpen()) {
            return;
        }

        long clientTime = message.getHeader().getSendTime();
        JSONObject msg = message.getMessageBody();
        //??????pipeline???????????????channel ????????????channel name
        JobStage jobStage = getJobStage(source.getConfigureName() + "_source_0");

//        // ?????????????????? traceId???????????? dipper_trace_monitor ???????????????????????????
//        // ????????????????????? traceId ??????
//        if (validTraceIds.contains(message.getHeader().getTraceId())){
//            // ??????????????????????????????
//            List<String> jobNameList = DBManager.querySQL("select job_name from dipper_job_stage where stage_name =" + "'" + jobStage.getStageName() + "'", String.class);
//            if (!jobNameList.isEmpty()){
//                String jobName = JSONObject.parseObject(jobNameList.get(0), HashMap.class).get("job_name").toString();
//                String key = jobName + "_" + message.getHeader().getTraceId();
//
//                // ?????? ??????+traceId ?????? dipper_trace_monitor ???
//                if (!jobMonitorCache.containsKey(key)){
//                    jobMonitorCache.put(key, "");
//                }else{
//                    // ?????? ??????+traceId ??? dipper_trace_monitor ???????????????????????????????????????
//                    // ?????????????????????????????????????????????
//                    TraceMonitorDO monitorDO = new TraceMonitorDO();
//                    monitorDO.setTraceId(message.getHeader().getTraceId());
//                    monitorDO.setJobName(jobName);
//                    // ???????????????????????????
//                    DBManager.executeSQL("delete from dipper_trace_monitor where trace_id=#{traceId} and job_name=#{jobName}", monitorDO);
//                }
//            }
//        }

        jobStage.getSafeInput().incrementAndGet();
        jobStage.setLastInputMsgObj(msg);
        if (clientTime != 0) {
            jobStage.setLastInputMsgTime(new Date(clientTime));
        } else {
            jobStage.setLastInputMsgTime(new Date());
        }
        jobStage.getSafeOutput().incrementAndGet();
        jobStage.setLastOutputMsgTime(new Date());

        String traceId = message.getHeader().getTraceId();
        if (validTraceIds.contains(traceId)) {
            if (!message.getHeader().isSystemMessage()) {
                // ?????? traceId
                // getTraceMonitor() ?????? traceCache ?????? traceMonitor???????????????????????????????????? traceCache ???
                TraceMonitorDO traceMonitor = getTraceMonitor(source.getConfigureName() + "_source_0", traceId);
                // ????????????????????????????????? stage
                traceMonitor.setStatus(1);
                traceMonitor.getSafeInput().incrementAndGet();
                traceMonitor.getSafeOutput().incrementAndGet();
                traceMonitor.setLastInputMsgTime(new Date());
                traceMonitor.setLastOutputMsgTime(new Date());
                traceMonitor.setInputLastMsg(msg.toJSONString());
                traceMonitor.setOutputLastMsg(msg.toJSONString());
                traceMonitor.setJobName(pipeline.getConfigureName());
            }
        }

    }

    /**
     * ?????? traceId ??????
     */
    public void reportInput(AbstractStage stage, IMessage message) {
        if (!isConsoleOpen()) {
            return;
        }

        JSONObject msg = message.getMessageBody();
        JobStage jobStage = getJobStage(stage.getLabel());
        jobStage.getSafeInput().incrementAndGet();
        jobStage.setLastInputMsgObj(msg);
        jobStage.setLastInputMsg(msg.toJSONString());
        jobStage.setLastInputMsgTime(new Date());

        String traceId = message.getHeader().getTraceId();
        String shuffleTraceId = msg.getString("SHUFFLE_TRACE_ID");
        if (validTraceIds.contains(traceId) || (shuffleTraceId != null && shuffleTraceId.contains(traceId))) {
            // getTraceMonitor() ?????? traceCache ?????? traceMonitor???????????????????????????????????? traceCache ???
            TraceMonitorDO traceMonitor = getTraceMonitor(stage.getLabel(), traceId);
            traceMonitor.getSafeInput().incrementAndGet();
            traceMonitor.setInputLastMsg(msg.toJSONString());
            traceMonitor.setLastInputMsgTime(new Date());
            traceMonitor.setJobName(stage.getPipeline().getConfigureName());
        }
    }

    /**
     * ?????? traceId ??????
     * status???
     * 0 ??????????????? stage
     * -1 ???????????? stage ????????????
     * 1 ???????????????
     */
    public void reportOutput(AbstractStage stage, IMessage message, int status, String exceptionMsg) {
        if (!isConsoleOpen()) {
            return;
        }

        JSONObject msg = message.getMessageBody();
        // ??? cache ????????? jobStage
        JobStage jobStage = getJobStage(stage.getLabel());
        if (status == MSG_FLOWED) {
            jobStage.getSafeOutput().incrementAndGet();
            jobStage.setLastOutputMsgTime(new Date());
        }
        String traceId = message.getHeader().getTraceId();
        String shuffleTraceId = msg.getString("SHUFFLE_TRACE_ID");

        // ???????????????????????? traceId ??????????????????????????????
        if (validTraceIds.contains(traceId) || (shuffleTraceId != null && shuffleTraceId.contains(traceId))) {
            // getTraceMonitor() ?????? traceCache ?????? traceMonitor???????????????????????????????????? traceCache ???
            TraceMonitorDO traceMonitor = getTraceMonitor(stage.getLabel(), traceId);
            // ?????? status ??????????????????????????????????????????????????? status ?????????????????????????????????????????????
            traceMonitor.setStatus(status);
            if (status == MSG_FILTERED) {
                traceMonitor.setExceptionMsg(exceptionMsg);
            } else if (status == MSG_FLOWED) {
                traceMonitor.getSafeOutput().incrementAndGet();
                traceMonitor.setOutputLastMsg(msg.toJSONString());
                traceMonitor.setLastOutputMsgTime(new Date());
            }
            traceMonitor.setJobName(stage.getPipeline().getConfigureName());
        }
    }

    public JobStage getJobStage(String uniqKey) {
//        String key = createKey(uniqKey);
        JobStage jobStage = cache.get(uniqKey);
        if (jobStage == null) {
            synchronized (uniqKey) {
                if (cache.get(uniqKey) == null) {
                    jobStage = new JobStage();
                    jobStage.setStageName(uniqKey);
                    cache.put(uniqKey, jobStage);
                }
            }
        }
        return jobStage;
    }

    public TraceMonitorDO getTraceMonitor(String uniqKey, String traceId) {
        String key = createKey(uniqKey, traceId);
        TraceMonitorDO traceMonitor = traceCache.get(key);
        if (traceMonitor == null) {
            synchronized (key) {
                if (traceCache.get(key) == null) {
                    traceMonitor = new TraceMonitorDO();
                    traceMonitor.setStageName(uniqKey);
                    traceMonitor.setTraceId(traceId);
                    traceCache.put(key, traceMonitor);
                }
            }
        }
        return traceMonitor;
    }

    public String createKey(String... uniqKeys) {
        //????????????????????????????????????
        StringBuffer sb = new StringBuffer(Thread.currentThread().getName());
        for (String key : uniqKeys) {
            sb.append(key);
        }
        return sb.toString();
    }

    public void queryValidTraceIds() {
        List<TraceIdsDO> traceIdsDOS = monitorDataSyncService.getTraceIds();
        if (traceIdsDOS != null && traceIdsDOS.size() > 0) {
            validTraceIds.clear();
            for (TraceIdsDO traceIdsDO : traceIdsDOS) {
                validTraceIds.add(traceIdsDO.getTraceId());
            }
        }

    }

    private boolean isConsoleOpen() {
        String configurableServiceType = ComponentCreator.getProperties().getProperty(DataSyncConstants.UPDATE_TYPE);
        if (DataSyncConstants.UPDATE_TYPE_ROCKETMQ.equalsIgnoreCase(configurableServiceType) ||
            DataSyncConstants.UPDATE_TYPE_HTTP.equalsIgnoreCase(configurableServiceType) ||
            DataSyncConstants.UPDATE_TYPE_DB.equalsIgnoreCase(configurableServiceType)) {
            return true;
        }
        return false;
    }

//    public void saveJobName(List<TraceMonitorDO> traceMonitorDOS){
//        if (traceMonitorDOS==null || traceMonitorDOS.size()==0){
//            return;
//        }
//        for (TraceMonitorDO traceMonitorDO : traceMonitorDOS){
//            // ??????????????????
//            String key = traceMonitorDO.getStageName();
//            if (jobNameCache.containsKey(key)){
//                jobNameCache.get(key).value = traceMonitorDO.getJobName();
//                moveToHead(jobNameCache.get(key));
//            }else{
//                if (jobNameCache.size() > MAX_SIZE){
//                    deleteNode();
//                }
//                // ?????????????????????????????????????????????????????????
//                ListNode node = new ListNode(key, traceMonitorDO.getJobName());
//                jobNameCache.put(key, node);
//                head.next.prev = node;
//                node.next = head.next;
//                node.prev = head;
//                head.next = node;
//            }
//        }
//    }
//
//    public String getJobName(String stageName){
//        if (!jobNameCache.containsKey(stageName)){
//            return null;
//        }
//        moveToHead(jobNameCache.get(stageName));
//        return jobNameCache.get(stageName).value;
//    }
//
//    public void moveToHead(ListNode node){
//        if (node.next != null){
//            node.prev.next = node.next;
//            node.next.prev = node.prev;
//        }else{
//            node.prev.next = null;
//        }
//        node.next = head.next;
//        head.next.prev = node;
//        head.next = node;
//        node.prev = head;
//    }
//
//    public void deleteNode(){
//        jobNameCache.remove(tail.prev.key);
//        tail.prev.prev.next = tail;
//        tail.prev = tail.prev.prev;
//    }
//
//    public void initJobCache(){
//        jobNameCache = new HashMap<>();
//        head = new ListNode();
//        tail = new ListNode();
//        head.next = tail;
//    }

    class ListNode {
        public String key;
        public String value;
        public ListNode prev;
        public ListNode next;

        public ListNode(String key, String value) {
            this.key = key;
            this.value = value;
        }

        public ListNode() {

        }
    }

    public static void main(String[] args) {
        long a = 6l;
        System.out.println((float) (a / ((10000 - 79) / 1000)));
        System.out.println((float) (a / 16));
        System.out.println((10000 - 79) * 1.0 / 1000);
//        Math.
    }

}


