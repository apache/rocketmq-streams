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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.monitor.DataSyncConstants;
import org.apache.rocketmq.streams.common.monitor.model.JobStage;
import org.apache.rocketmq.streams.common.monitor.model.TraceIdsDO;
import org.apache.rocketmq.streams.common.monitor.model.TraceMonitorDO;
import org.apache.rocketmq.streams.common.monitor.service.MonitorDataSyncService;
import org.apache.rocketmq.streams.common.utils.IPUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;

public class RocketMQMonitorDataSyncImpl implements MonitorDataSyncService {

    protected final Log LOG = LogFactory.getLog(RocketMQMonitorDataSyncImpl.class);

    public static final String QUERYVALIDTRACEID = "queryValidTraceId";
    public static final String UPDATEJOBSTAGE = "updateJobStage";
    public static final String INSERTTRACEMONITOR = "insertTraceMonitor";

    private Updater updater;

    private List<TraceIdsDO> traceIdsDOS;

    public RocketMQMonitorDataSyncImpl() {
        updater = new Updater();
        updater.init();
    }

    @Override
    public List<TraceIdsDO> getTraceIds() {
        return traceIdsDOS;
    }

    @Override
    public void updateJobStage(Collection<JobStage> jobStages) {
        if (jobStages.isEmpty()) {
            return;
        }
        JSONObject object = new JSONObject();
        object.put("operate", UPDATEJOBSTAGE);
        object.put("data", jobStages);
        updater.sendMsg(object);

    }

    @Override
    public void addTraceMonitor(TraceMonitorDO traceMonitorDO) {
        JSONObject object = new JSONObject();
        object.put("operate", INSERTTRACEMONITOR);
        object.put("data", traceMonitorDO);
        updater.sendMsg(object);

    }

    public String updateTraceIds(JSONObject object) {
        List<TraceIdsDO> ids = new ArrayList<>();
        JSONArray array = object.getJSONArray("data");
        if (array != null && array.size() > 0) {
            for (int i = 0; i < array.size(); i++) {
                ids.add(array.getObject(i, TraceIdsDO.class));
            }
        }
        this.traceIdsDOS = ids;

        return Updater.RESULT_SUCCESS;
    }

    private String dealMessage(String message) {
        JSONObject object = JSON.parseObject(message);
        String operate = object.getString("operate");
//        Long operateTime = object.getLong("operate_time");
        if (QUERYVALIDTRACEID.equalsIgnoreCase(operate)) {
            return updateTraceIds(object);
        }
        return Updater.RESULT_SUCCESS;
    }

    class Updater {
        public static final String RESULT_SUCCESS = "success";
        public static final String RESULT_FAILED = "failed";

        protected Long pullIntervalMs;
        protected String ruleUpTopic = ComponentCreator.getProperties().getProperty(DataSyncConstants.RULE_UP_TOPIC);
        protected String ruleDownTopic = ComponentCreator.getProperties().getProperty(DataSyncConstants.RULE_DOWN_TOPIC);
        protected String tags = "*";
        public String CHARSET = "UTF-8";

        protected transient DefaultMQProducer producer;

        public void init() {
            initConsumer();
            initProducer();

        }

        protected DefaultMQPushConsumer initConsumer() {
            try {
//                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(IPUtil.getLocalIdentification().replaceAll("\\.", "_"));
                DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("jobconfig_comsumer");
                if (pullIntervalMs != null) {
                    consumer.setPullInterval(pullIntervalMs);
                }
//                consumer.setNamesrvAddr(this.namesrvAddr);
                consumer.subscribe(ruleDownTopic, tags);
                consumer.registerMessageListener((MessageListenerOrderly) (msgs, context) -> {
                    try {
                        int i = 0;
                        for (MessageExt msg : msgs) {
                            String ruleMsg = new String(msg.getBody(), CHARSET);
                            dealMessage(ruleMsg);
                        }
                    } catch (Exception e) {
                        LOG.error("consume message from rocketmq error " + e, e);
                        e.printStackTrace();
                    }
                    return ConsumeOrderlyStatus.SUCCESS;// 返回消费成功
                });

                consumer.start();
                return consumer;
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("start metaq channel error " + ruleUpTopic, e);
            }
        }

        protected void initProducer() {
            producer = new DefaultMQProducer(RuntimeUtil.getDipperInstanceId() + "producer");
            try {
                producer.start();
            } catch (Exception e) {
                LOG.error("init producer error " + e, e);
                throw new RuntimeException("init producer error, msg=" + e.getMessage(), e);
            }
        }

        protected void sendMsg(JSONObject msg) {
            sendMsg(msg, ruleUpTopic);
        }

        protected void sendMsg(JSONObject msg, String topic) {
            try {
                Message message = new Message(topic, tags, null, msg.toJSONString().getBytes("UTF-8"));
                producer.send(message);
            } catch (Exception e) {
                LOG.error("updater sendMsg error: ", e);
                e.printStackTrace();
            }
        }

    }
}
