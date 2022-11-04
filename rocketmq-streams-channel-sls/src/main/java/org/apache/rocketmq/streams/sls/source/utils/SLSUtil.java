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
package org.apache.rocketmq.streams.sls.source.utils;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.LogStore;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.GetLogStoreResponse;
import com.aliyun.openservices.log.response.GetProjectResponse;
import java.util.Iterator;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLSUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(SLSUtil.class);

    public static LogItem createLogItem(IMessage msg, String logTimeFieldName) {
        Map<String, Object> fieldName2Value = null;
        Object object = msg.getMessageValue();
        if (!(object instanceof Map)) {
            throw new RuntimeException("can not support this class ,expect Map, actual is " + object.getClass());
        } else {
            fieldName2Value = (Map<String, Object>)object;
        }
        LogItem logItem = null;
        String itemTime = "";
        if (StringUtil.isNotEmpty(logTimeFieldName)) {
            Object start_time = fieldName2Value.get(logTimeFieldName);
            itemTime = (String)start_time;
        } else {
            //            LOG.error("logTimeFieldName is null, maybe can not search by time ");
        }
        //TODO delete it
        if (fieldName2Value != null) {
            fieldName2Value.put("ability_source", "sql-engine");
        }
        if (!"".equals(itemTime)) {
            // 创建logItem
            try {
                int logTime2 = (int)(DateUtil.StringToLong(itemTime) / 1000L);
                if (logTime2 == 0L) {// 时间转换出错，默认当前时间
                    // 默认当前时间
                    logItem = new LogItem();
                } else {
                    logItem = new LogItem(logTime2);
                }
            } catch (Exception e) {
                // 默认当前时间
                logItem = new LogItem();
            }
        } else {
            logItem = new LogItem();
        }

        Iterator<Map.Entry<String, Object>> it = fieldName2Value.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String value = entry.getValue() == null ? "" : String.valueOf(entry.getValue());
            if (value == null) {
                //                LOG.error("SLSDataSource value is null");
                value = "";
            }

            logItem.PushBack(entry.getKey(), value);
        }
        return logItem;
    }

    public static boolean existProject(Client client, String project) {
        try {
            GetProjectResponse response = client.GetProject(project);
            return response != null;
        } catch (LogException e) {
            LOGGER.error("existProject is error", e);
            return false;
        }
    }

    public static void createProject(Client client, String project) {
        try {
            client.CreateProject(project, "");
        } catch (LogException e) {
            LOGGER.error("createProject is error", e);
            throw new RuntimeException(e);
        }
    }

    public static boolean existLogstore(Client client, String project, String logstore) {
        try {
            GetLogStoreResponse response = client.GetLogStore(project, logstore);
            return response != null && response.GetLogStore() != null;
        } catch (LogException e) {
            LOGGER.error("existLogstore is error", e);
            return false;
        }
    }

    public static void createLogstore(Client client, String project, String logstore, int ttl, int shardCount) {
        try {
            LogStore logStore = new LogStore(logstore, ttl, shardCount);
            client.CreateLogStore(project, logStore);
        } catch (LogException e) {
            LOGGER.error("createLogstore is error", e);
            throw new RuntimeException(e);
        }
    }
}
