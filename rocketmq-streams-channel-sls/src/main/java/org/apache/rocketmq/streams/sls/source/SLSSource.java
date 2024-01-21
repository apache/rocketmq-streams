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
package org.apache.rocketmq.streams.sls.source;

import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.ConsumerGroup;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.response.ListConsumerGroupResponse;
import com.aliyun.openservices.loghub.client.ClientWorker;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.channel.source.AbstractPushSource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.sls.sink.SLSSplit;
import org.apache.rocketmq.streams.sts.StsIdentity;
import org.apache.rocketmq.streams.sts.StsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 做为metaq的一个消息队列。每次增加一个队列只需要在数据库中增加一条Channel记录即可。 记录中的字端代表了metaq队列的参数 要求必须有无参数构造函数
 */
public class SLSSource extends AbstractPushSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(SLSSource.class);

    private static final long serialVersionUID = 5429201258366881915L;
    private final transient LogHubConfig.ConsumePosition cursorPosition = LogHubConfig.ConsumePosition.END_CURSOR;

    @ENVDependence public String project;

    @ENVDependence public String logStore;
    @ENVDependence private String endPoint;
    @ENVDependence private String accessId;
    @ENVDependence private String accessKey;
    @ENVDependence private boolean isSts;
    @ENVDependence private String stsRoleArn;
    @ENVDependence private String stsAssumeRoleFor;
    @ENVDependence private String stsSessionPrefix;
    @ENVDependence private int stsExpireSeconds = 86400;
    @ENVDependence private String ramEndpoint = "sts-inner.aliyuncs.com";
    private transient StsService stsService;
    private transient ClientWorker clientWorker;
    // worker 向服务端汇报心跳的时间间隔，单位是毫秒，建议取值 10000ms。
    private long heartBeatIntervalMillis = 10000;

    // 是否按序消费
    private boolean consumeInOrder = true;

    private transient volatile boolean isFinished = false;                                     // 如果消息被销毁，会通过这个标记停止消息的消费

    //sls数据保存周期，默认7天
    private int ttl = 7;

    //sls shard个数 默认8
    private int shardCount = 8;

    public SLSSource() {
    }

    public SLSSource(String endPoint, String project, String logStore, String accessId, String accessKey, String groupName) {
        this.endPoint = endPoint;
        this.project = project;
        this.logStore = logStore;
        this.accessId = accessId;
        this.accessKey = accessKey;
        this.groupName = groupName;
    }

    public static void main(String[] args) {
        System.out.println(Runtime.getRuntime().availableProcessors());
    }

    public static StsIdentity getIdentity() {
        return null;
    }

    @Override protected boolean initConfigurable() {
        if (StringUtil.isEmpty(endPoint) || StringUtil.isEmpty(accessId) || StringUtil.isEmpty(accessKey) || StringUtil.isEmpty(logStore)) {
            return false;
        }
        return super.initConfigurable();
    }

    private boolean initSts() {
        if (isSts) {
            if (StringUtil.isEmpty(stsRoleArn) || StringUtil.isEmpty(stsAssumeRoleFor)) {
                return false;
            }
            if (this.stsService == null) {
                this.stsService = new StsService();
                this.stsService.setAccessId(accessId);
                this.stsService.setAccessKey(accessKey);
                this.stsService.setRamEndPoint(ramEndpoint);
                this.stsService.setStsExpireSeconds(stsExpireSeconds);
                this.stsService.setStsSessionPrefix(stsSessionPrefix);
                this.stsService.setRoleArn(stsRoleArn);
                this.stsService.setStsAssumeRoleFor(stsAssumeRoleFor);
            }
        }
        return true;
    }

    @Override public boolean startSource() {
        initSts();
        startWork();
        startStsRefreshAsync();
        return true;
    }

    protected LogHubConfig createLogHubConfig(String accessId, String accessKey, String groupName, String endPoint, String project, String logStore) throws ExecutionException {
        String localAccessId = accessId;
        String localAccessKey = accessKey;
        LogHubConfig config;
        if (isSts) {
            StsIdentity stsIdentity = this.stsService.getStsIdentity();
            localAccessId = stsIdentity.getAccessKeyId();
            localAccessKey = stsIdentity.getAccessKeySecret();
            String localToken = stsIdentity.getSecurityToken();
            config = new LogHubConfig(groupName, UUID.randomUUID().toString(), endPoint, project, logStore, localAccessId, localAccessKey, cursorPosition);
            config.setStsToken(localToken);
        } else {
            config = new LogHubConfig(groupName, UUID.randomUUID().toString(), endPoint, project, logStore, localAccessId, localAccessKey, cursorPosition);
        }
        config.setMaxFetchLogGroupSize(getMaxFetchLogGroupSize());
        LOGGER.info("[{}][{}] SLSSource_IsSts({})", IdUtil.instanceId(), getName(), isSts);
        return config;
    }

    protected Client createClient() throws ExecutionException {
        Client client;
        if (isSts) {
            StsIdentity stsIdentity = this.stsService.getStsIdentity();
            client = new Client(endPoint, stsIdentity.getAccessKeyId(), stsIdentity.getAccessKeySecret());
            client.setSecurityToken(stsIdentity.getSecurityToken());
        } else {
            client = new Client(endPoint, accessId, accessKey);
        }
        return client;
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        try {
            List<Shard> shards = createClient().ListShard(project, logStore).GetShards();
            List<ISplit<?, ?>> splits = new ArrayList<>();
            for (Shard shard : shards) {
                splits.add(new SLSSplit(shard));
            }
            return splits;
        } catch (ExecutionException ex) {
            throw new RuntimeException("Error while listing shards", ex);
        } catch (LogException e) {
            throw new RuntimeException(e);
        }
    }

    protected void startStsRefreshAsync() {
        try {
            if (isSts) {
                Runnable stsRefreshTask = () -> {
                    try {
                        StsIdentity stsIdentity = stsService.getStsIdentity();
                        while (clientWorker == null) {
                            Thread.sleep(100);
                        }
                        clientWorker.SwitchClient(stsIdentity.getAccessKeyId(), stsIdentity.getAccessKeySecret(), stsIdentity.getSecurityToken());
                    } catch (ExecutionException | InterruptedException e) {
                        e.printStackTrace();
                    }
                };

                ScheduleFactory.getInstance().execute(getNameSpace() + "-" + getName() + "-source_sts_schedule", stsRefreshTask, 0, stsService.getRefreshTimeSecond(), TimeUnit.SECONDS);
            }
        } catch (Exception e) {
            throw new RuntimeException("Start_Sts_Refresh_Sync_Error ", e);
        }
    }

    protected void startWork() {
        try {
            String runtimeEnv = this.getConfiguration().getProperty(ConfigurationKey.RUNTIME_ENV);
            if (runtimeEnv == null) {
                runtimeEnv = "default";
            }
            LogHubConfig config = createLogHubConfig(this.accessId, this.accessKey, this.groupName + "_" + runtimeEnv, this.endPoint, this.project, this.logStore);
            if (this.clientWorker == null) {
                this.clientWorker = new ClientWorker(new LogHubProcessorFactory(this), config);
            }
        } catch (Exception e) {
            LOGGER.error("[{}][{}] Start_Sls_Channel_Error: {}|{}|{}", IdUtil.instanceId(), getName(), project, logStore, groupName, e);
            try {
                List<String> existConsumerGroup = Lists.newArrayList();
                ListConsumerGroupResponse groupResponse = createClient().ListConsumerGroup(project, logStore);
                List<ConsumerGroup> consumerGroups = groupResponse.GetConsumerGroups();
                if (consumerGroups != null) {
                    for (ConsumerGroup consumerGroup : consumerGroups) {
                        String consumerGroupName = consumerGroup.getConsumerGroupName();
                        LOGGER.info("[{}][{}] List_Consumer_Group_Success:{}|{}|{}", IdUtil.instanceId(), getName(), project, logStore, consumerGroupName);
                        if (consumerGroupName.length() == 15) { //siem期初生成的consumerGroup
                            existConsumerGroup.add(consumerGroupName);
                        }
                    }
                }

                if (e.getMessage().contains("consumer group quota exceed")) {
                    if (!existConsumerGroup.isEmpty()) {
                        Collections.sort(existConsumerGroup);
                        String newConsumerGroup = existConsumerGroup.get(0);
                        LogHubConfig config = createLogHubConfig(this.accessId, this.accessKey, newConsumerGroup, this.endPoint, this.project, this.logStore);
                        if (this.clientWorker == null) {
                            this.clientWorker = new ClientWorker(new LogHubProcessorFactory(this), config);
                        }
                        LOGGER.info("[{}][{}] Consumer_Group_Fixed_To:{}|{}|{}", IdUtil.instanceId(), getName(), project, logStore, newConsumerGroup);
                    }
                }

            } catch (Exception ex) {
                LOGGER.error("[{}][{}] List_Consumer_Group_Error:{}|{}", IdUtil.instanceId(), getName(), project, logStore, ex);
            }
        }
        if (this.clientWorker != null) {
            Thread thread = new Thread(this.clientWorker);
            thread.start();
        } else {
            LOGGER.error("[{}][{}] Start_Sls_Error_Client_Worker_Is_Null: {}|{}|{}", IdUtil.instanceId(), getName(), project, logStore, groupName);
        }
    }

    @Override public void destroySource() {
        isFinished = true;
        if (this.clientWorker != null) {
            this.clientWorker.shutdown();
            this.clientWorker = null;
        }
        ScheduleFactory.getInstance().cancel(getNameSpace() + "-" + getName() + "-source_sts_schedule");
    }

    public LogHubConfig.ConsumePosition getCursorPosition() {
        return cursorPosition;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getLogStore() {
        return logStore;
    }

    public void setLogStore(String logStore) {
        this.logStore = logStore;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public long getHeartBeatIntervalMillis() {
        return heartBeatIntervalMillis;
    }

    public void setHeartBeatIntervalMillis(long heartBeatIntervalMillis) {
        this.heartBeatIntervalMillis = heartBeatIntervalMillis;
    }

    public boolean isConsumeInOrder() {
        return consumeInOrder;
    }

    public void setConsumeInOrder(boolean consumeInOrder) {
        this.consumeInOrder = consumeInOrder;
    }

    @Override public boolean isFinished() {
        return isFinished;
    }

    public void setFinished(boolean finished) {
        isFinished = finished;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public int getShardCount() {
        return shardCount;
    }

    public void setShardCount(int shardCount) {
        this.shardCount = shardCount;
    }

    public boolean isSts() {
        return isSts;
    }

    public void setSts(boolean sts) {
        isSts = sts;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }

    public void setStsRoleArn(String stsRoleArn) {
        this.stsRoleArn = stsRoleArn;
    }

    public String getStsAssumeRoleFor() {
        return stsAssumeRoleFor;
    }

    public void setStsAssumeRoleFor(String stsAssumeRoleFor) {
        this.stsAssumeRoleFor = stsAssumeRoleFor;
    }

    public String getStsSessionPrefix() {
        return stsSessionPrefix;
    }

    public void setStsSessionPrefix(String stsSessionPrefix) {
        this.stsSessionPrefix = stsSessionPrefix;
    }

    public int getStsExpireSeconds() {
        return stsExpireSeconds;
    }

    public void setStsExpireSeconds(int stsExpireSeconds) {
        this.stsExpireSeconds = stsExpireSeconds;
    }

    public String getRamEndpoint() {
        return ramEndpoint;
    }

    public void setRamEndpoint(String ramEndpoint) {
        this.ramEndpoint = ramEndpoint;
    }

    @Override protected boolean hasListenerSplitChanged() {
        return true;
    }

}