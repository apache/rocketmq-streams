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
package org.apache.rocketmq.streams.common.monitor.group;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.monitor.MonitorFactory;

public class MonitorCommander {

    private static final Log logger = LogFactory.getLog(MonitorCommander.class);

    private final static MonitorCommander monitorManager = new MonitorCommander();

    private final static MetaData metaData = new MetaData();

    static {
        metaData.setTableName("mq_monitor_data");
        metaData.setIdFieldName("id");
        metaData.getMetaDataFields().add(createMetaDataField("id", new LongDataType()));
        metaData.getMetaDataFields().add(createMetaDataField("name", new StringDataType()));
        metaData.getMetaDataFields().add(createMetaDataField("count", new StringDataType()));
        metaData.getMetaDataFields().add(createMetaDataField("max", new IntDataType()));
        metaData.getMetaDataFields().add(createMetaDataField("min", new StringDataType()));
        metaData.getMetaDataFields().add(createMetaDataField("avg", new StringDataType()));
        metaData.getMetaDataFields().add(createMetaDataField("errorCount", new BooleanDataType()));
        metaData.getMetaDataFields().add(createMetaDataField("slowCount", new BooleanDataType()));
    }

    private final Object object = new Object();
    private Object initObject = new Object();
    private boolean inited = false;
    private Map<String, GroupedMonitorInfo> groupedMonitorInfoMap = new HashMap<String, GroupedMonitorInfo>();
    private List<ISink> outputDataSourceList = new ArrayList<ISink>();

    public static MonitorCommander getInstance() {
        return monitorManager;
    }

    /**
     * 因为所有字段都是字符类型，
     *
     * @param name
     * @return
     */
    private static MetaDataField createMetaDataField(String name, DataType dataType) {
        MetaDataField metaDataField = new MetaDataField();
        metaDataField.setFieldName(name);
        metaDataField.setIsRequired(false);
        metaDataField.setDataType(dataType);
        return metaDataField;
    }

    public void init(ISink... outputDataSource) {
        if (inited) {
            return;
        }
        synchronized (initObject) {
            if (inited) {
                return;
            }
            for (ISink source : outputDataSource) {
                if (source != null) {
                    outputDataSourceList.add(source);
                }
            }
            ScheduledExecutorService monitorService = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.Builder().namingPattern("monitor-schedule-pool-%d").build());
            monitorService.scheduleWithFixedDelay(() -> {
                Map<String, GroupedMonitorInfo> groupedMap;
                synchronized (object) {
                    groupedMap = groupedMonitorInfoMap;
                    groupedMonitorInfoMap = new HashMap<>();
                }
                ISink loggerOutputDataSource = MonitorFactory.createOrGetLogOutputDatasource(
                    "data_process_info");
                for (String key : groupedMap.keySet()) {
                    GroupedMonitorInfo gmi = groupedMap.get(key);
                    JSONObject result = new JSONObject();
                    result.put("name", gmi.getName());
                    result.put("count", gmi.getCount());
                    result.put("max", gmi.getMax());
                    result.put("min", gmi.getMin());
                    result.put("avg", gmi.getAvg());
                    result.put("errorCount", gmi.getErrorCount());
                    result.put("slowCount", gmi.getSlowCount());
                    //本地打印
                    loggerOutputDataSource.batchAdd(new Message(result));
                    loggerOutputDataSource.flush();
                    //远程输出
                    for (ISink source : outputDataSourceList) {
                        source.batchAdd(new Message(result));
                    }
                }
                //flush出去
                loggerOutputDataSource.flush();
                for (ISink source : outputDataSourceList) {
                    source.flush();
                }
            }, 0, 60, TimeUnit.SECONDS);
            inited = true;
        }
    }

    public void finishMonitor(String groupName, IMonitor monitor) {
        try {
            finishMonitor(groupName, monitor, 1);
        } catch (Exception e) {
            logger.error("finishMonitor error " + e.getMessage(), e);
        }
    }

    public void finishMonitor(String groupName, IMonitor monitor, int level) {
        if (level > 10) {
            logger.error(String.format("level>10, groupName=%s, level=%s", groupName, level));
            return;
        }
        String type = monitor.getType();
        if (IMonitor.TYPE_STARTUP.equals(type) || IMonitor.TYPE_HEARTBEAT.equals(type)) {
            monitor.output();
        } else if (IMonitor.TYPE_DATAPROCESS.equals(type)) {
            //if (monitor.isError()) {
            //    //有错误 则直接发送出去
            //    monitor.output();
            //}else if(monitor.isSlow()){
            //    monitor.output();
            //}
            //synchronized (object) {
            monitor.output();
            String name = monitor.getName();
            //                GroupedMonitorInfo groupedMonitorInfo = groupedMonitorInfoMap.get(name);
            //                if (groupedMonitorInfo == null) {
            //                    groupedMonitorInfo = new GroupedMonitorInfo();
            //                    groupedMonitorInfo.setName(name);
            //                }
            //                groupedMonitorInfo.addMonitor(monitor);
            //                groupedMonitorInfoMap.put(name, groupedMonitorInfo);
            List<IMonitor> list = monitor.getChildren();
            if (list != null) {
                int count = 0;
                int allCount = 0;
                for (IMonitor iMonitor : list) {
                    allCount++;
                    if (allCount > 100) {
                        break;
                    }
                    iMonitor.setType(monitor.getType());
                    //防止list数据太大，导致文件输出过大
                    if (iMonitor.isError() || iMonitor.isSlow()) {
                        finishMonitor(name + "#" + iMonitor.getName(), iMonitor, level + 1);
                        count++;
                    }
                    if (count > 20) {
                        break;
                    }
                }
            }
            //            }
        } else {
            logger.error(String.format("unknown monitor type groupName=%s, monitorName=%s, monitorType=%s", groupName,
                monitor.getName(), monitor.getType()));
            monitor.output();
        }

    }

    public void output() {

    }

}
