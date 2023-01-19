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
package org.apache.rocketmq.streams.core.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.topic.UpdateStaticTopicSubCommand;
import org.apache.rocketmq.tools.command.topic.UpdateTopicSubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RocketMQUtil {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQUtil.class.getName());

    private static final List<String> existTopic = new ArrayList<>();

    //neither static topic nor compact topic. expansion with source topic.
    public static void createNormalTopic(DefaultMQAdminExt mqAdmin, String topicName, int totalQueueNum, Set<String> clusters) throws Exception {
        if (check(mqAdmin, topicName)) {
            logger.info("topic[{}] already exist.", topicName);
            return;
        }

        if (clusters == null || clusters.size() == 0) {
            clusters = getCluster(mqAdmin);
        }


        for (String cluster : clusters) {
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdmin, cluster);

            int remainder = totalQueueNum % masterSet.size();
            if (remainder != 0) {
                String temp = String.format("can not create topic:%s, total num=%s, master num=%s", topicName, totalQueueNum, masterSet.size());
                logger.warn(temp);
            }

            int queueNumInEachBroker = totalQueueNum / masterSet.size();
            TopicConfig topicConfig = new TopicConfig(topicName, queueNumInEachBroker, queueNumInEachBroker, PermName.PERM_READ | PermName.PERM_WRITE);

            if (remainder == 0) {
                for (String addr : masterSet) {
                    mqAdmin.createAndUpdateTopicConfig(addr, topicConfig);
                    logger.info("create topic to broker:{} cluster:{}, success.", addr, cluster);
                }
            } else {
                String[] masterArray = masterSet.toArray(new String[]{});

                topicConfig = new TopicConfig(topicName, queueNumInEachBroker + remainder,
                        queueNumInEachBroker + remainder, PermName.PERM_READ | PermName.PERM_WRITE);
                mqAdmin.createAndUpdateTopicConfig(masterArray[0], topicConfig);

                for (int i = 1; i < masterArray.length; i++) {
                    topicConfig = new TopicConfig(topicName, queueNumInEachBroker, queueNumInEachBroker, PermName.PERM_READ | PermName.PERM_WRITE);
                    mqAdmin.createAndUpdateTopicConfig(masterArray[0], topicConfig);
                }
            }

        }
    }

    //used in RSQLDB,maybe.
    public static void createStaticCompactTopic(DefaultMQAdminExt mqAdmin, String topicName, int totalQueueNum, Set<String> clusters) throws Exception {
        if (check(mqAdmin, topicName)) {
            logger.info("topic[{}] already exist.", topicName);
            return;
        }

        if (clusters == null || clusters.size() == 0) {
            clusters = getCluster(mqAdmin);
        }


        for (String cluster : clusters) {
            createStaticTopicWithCommand(topicName, totalQueueNum, new HashSet<>(), cluster, mqAdmin.getNamesrvAddr());
            logger.info("【step 1】create static topic:[{}] in cluster:[{}] success, logic queue num:[{}].", topicName, cluster, totalQueueNum);

            update2CompactTopicWithCommand(topicName, totalQueueNum, cluster, mqAdmin.getNamesrvAddr());
            logger.info("【step 2】update static topic to compact topic success. topic:[{}], cluster:[{}]", topicName, cluster);
        }

        existTopic.add(topicName);
        logger.info("create static-compact topic [{}] success, queue num [{}]", topicName, totalQueueNum);
    }

    public static void createStaticTopic(DefaultMQAdminExt mqAdmin, String topicName, int queueNum) throws Exception {
        if (check(mqAdmin, topicName)) {
            logger.info("topic[{}] already exist.", topicName);
            return;
        }

        Set<String> clusters = getCluster(mqAdmin);
        for (String cluster : clusters) {
            createStaticTopicWithCommand(topicName, queueNum, new HashSet<>(), cluster, mqAdmin.getNamesrvAddr());
            logger.info("create static topic:[{}] in cluster:[{}] success, logic queue num:[{}].", topicName, cluster, queueNum);
        }

        existTopic.add(topicName);
    }

    private static void createStaticTopicWithCommand(String topic, int totalQueueNum, Set<String> brokers, String cluster, String nameservers) throws Exception {
        UpdateStaticTopicSubCommand cmd = new UpdateStaticTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] args;
        if (cluster != null) {
            args = new String[]{
                    "-c", cluster,
                    "-t", topic,
                    "-qn", String.valueOf(totalQueueNum),
                    "-n", nameservers
            };
        } else {
            String brokerStr = String.join(",", brokers);
            args = new String[]{
                    "-b", brokerStr,
                    "-t", topic,
                    "-qn", String.valueOf(totalQueueNum),
                    "-n", nameservers
            };
        }

        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + cmd.commandName(), args, cmd.buildCommandlineOptions(options), new PosixParser());

        String namesrvAddr = commandLine.getOptionValue('n');
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);

        cmd.execute(commandLine, options, null);
    }

    private static void update2CompactTopicWithCommand(String topic, int queueNum, String cluster, String nameservers) throws Exception {
        UpdateTopicSubCommand command = new UpdateTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] args = new String[]{
                "-c", cluster,
                "-t", topic,
                "-r", String.valueOf(queueNum),
                "-w", String.valueOf(queueNum),
                "-n", nameservers
//                todo 发布版本还不支持
//                , "-a", "+delete.policy=COMPACTION"
        };

        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + command.commandName(), args, command.buildCommandlineOptions(options), new PosixParser());
        String namesrvAddr = commandLine.getOptionValue('n');
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);

        command.execute(commandLine, options, null);
    }


    public static Set<String> getCluster(DefaultMQAdminExt mqAdmin) throws Exception {
        ClusterInfo clusterInfo = mqAdmin.examineBrokerClusterInfo();
        return clusterInfo.getClusterAddrTable().keySet();
    }

    private static boolean check(DefaultMQAdminExt mqAdmin, String topicName) {
        if (existTopic.contains(topicName)) {
            return true;
        }

        try {
            mqAdmin.examineTopicRouteInfo(topicName);
            existTopic.add(topicName);
            return true;
        } catch (RemotingException | InterruptedException e) {
            logger.error("examine topic route info error.", e);
            throw new RuntimeException("examine topic route info error.", e);
        } catch (MQClientException exception) {
            if (exception.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                logger.info("topic[{}] does not exist, create it.", topicName);
            } else {
                throw new RuntimeException(exception);
            }
        }
        return false;
    }

    public static boolean checkWhetherExist(String topic) {
        return existTopic.contains(topic);
    }
}
