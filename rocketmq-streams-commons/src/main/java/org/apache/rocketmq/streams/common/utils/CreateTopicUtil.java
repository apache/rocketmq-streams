package org.apache.rocketmq.streams.common.utils;
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

import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;

import java.util.HashMap;
import java.util.Set;

public class CreateTopicUtil {

    public static boolean create(String clusterName, String topic, int queueNum, String namesrv) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt();
        defaultMQAdminExt.setVipChannelEnabled(false);
        defaultMQAdminExt.setNamesrvAddr(namesrv);
        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        defaultMQAdminExt.setAdminExtGroup(topic.trim());
        TopicConfig topicConfig = new TopicConfig();
        topicConfig.setReadQueueNums(queueNum);
        topicConfig.setWriteQueueNums(queueNum);
        topicConfig.setTopicName(topic.trim());

        HashMap<String, String> temp = new HashMap<>();
        temp.put("+delete.policy", "COMPACTION");
//        topicConfig.setAttributes(temp);

        try {
            defaultMQAdminExt.start();
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(defaultMQAdminExt, clusterName);
            for (String master : masterSet) {
                defaultMQAdminExt.createAndUpdateTopicConfig(master, topicConfig);
            }

            return true;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
