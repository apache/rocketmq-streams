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
package org.apache.rocketmq.streams.sls.sink;

import com.aliyun.openservices.aliyun.log.producer.LogProducer;
import com.aliyun.openservices.aliyun.log.producer.ProducerConfig;
import com.aliyun.openservices.aliyun.log.producer.ProjectConfig;
import com.aliyun.openservices.aliyun.log.producer.errors.ProducerException;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.LogItem;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.loghub.client.config.LogHubConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.sls.source.utils.SLSUtil;
import org.apache.rocketmq.streams.sts.StsIdentity;
import org.apache.rocketmq.streams.sts.StsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 做为metaq的一个消息队列。每次增加一个队列只需要在数据库中增加一条Channel记录即可。 记录中的字端代表了metaq队列的参数 要求必须有无参数构造函数
 */
public class SLSSink extends AbstractSupportShuffleSink {
    /**
     * sls shard个数 默认8
     */
    private static final int SHARD_COUNT = 8;
    private static final long serialVersionUID = 5429201258366881915L;
    private static final Logger LOGGER = LoggerFactory.getLogger(SLSSink.class);
    private static final int MAX_LIMIT_COUNT = 4000;
    private final transient LogHubConfig.ConsumePosition cursorPosition = LogHubConfig.ConsumePosition.END_CURSOR;
    private final transient ReadWriteLock lock = new ReentrantReadWriteLock();
    @ENVDependence public String project;
    @ENVDependence public String logStore;
    @ENVDependence protected String source;

    @ENVDependence protected String topic;

    protected String logTimeFieldName;
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
    private transient Client client = null;
    // worker 向服务端汇报心跳的时间间隔，单位是毫秒，建议取值 10000ms。
    private long heartBeatIntervalMillis = 10000;
    // 是否按序消费
    private boolean consumeInOrder = true;
    private transient boolean isFinished = false;                                     // 如果消息被销毁，会通过这个标记停止消息的消费
    //sls数据保存周期，默认7天
    private int ttl = 7;
    private transient LogProducer logProducer;

    public SLSSink() {
    }

    public SLSSink(String endPoint, String project, String logStore, String accessId, String accessKey) {
        this.endPoint = endPoint;
        this.project = project;
        this.logStore = logStore;
        this.accessId = accessId;
        this.accessKey = accessKey;
    }

    @Override protected boolean initConfigurable() {
        if (StringUtil.isEmpty(endPoint) || StringUtil.isEmpty(accessId) || StringUtil.isEmpty(accessKey) || StringUtil.isEmpty(logStore)) {
            return false;
        }
        boolean b = initSts();
        if (!b) {
            return false;
        }
        try {
            this.client = createClient();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        if (this.logProducer == null) {
            try {
                ProjectConfig projectConfig = createProjectConfig();
                this.logProducer = new LogProducer(new ProducerConfig());
                this.logProducer.putProjectConfig(projectConfig);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        this.startRefreshAsync();
        return super.initConfigurable();
    }

    @Override public int getSplitNum() {
        return getSplitList().size();
    }

    @Override public List<ISplit<?, ?>> getSplitList() {
        lock.readLock().lock();
        try {
            List<Shard> shards = client.ListShard(project, logStore).GetShards();
            List<ISplit<?, ?>> splits = new ArrayList<>();
            for (Shard shard : shards) {
                splits.add(new SLSSplit(shard));
            }
            return splits;
        } catch (LogException ex) {
            throw new RuntimeException("Error while listing shards", ex);
        } finally {
            lock.readLock().unlock();
        }
    }

    private boolean initSts() {
        if (isSts) {
            if (StringUtil.isEmpty(stsRoleArn) || StringUtil.isEmpty(stsAssumeRoleFor)) {
                return false;
            }
            stsService = new StsService();
            stsService.setAccessId(accessId);
            stsService.setAccessKey(accessKey);
            stsService.setRamEndPoint(ramEndpoint);
            stsService.setStsExpireSeconds(stsExpireSeconds);
            stsService.setStsSessionPrefix(stsSessionPrefix);
            stsService.setRoleArn(stsRoleArn);
            stsService.setStsAssumeRoleFor(stsAssumeRoleFor);
        }
        return true;
    }

    protected void startRefreshAsync() {
        if (isSts) {
            Runnable stsRefreshTask = () -> {
                Client c = null;
                ProjectConfig projectConfig = null;
                try {
                    c = createClient();
                    projectConfig = createProjectConfig();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
                lock.writeLock().lock();
                client = c;
                lock.writeLock().unlock();
                logProducer.putProjectConfig(projectConfig);

            };
            ScheduleFactory.getInstance().execute(getNameSpace() + "-" + getName() + "-sink_sts_schedule", stsRefreshTask, stsService.getRefreshTimeSecond(), stsService.getRefreshTimeSecond(), TimeUnit.SECONDS);
        }
    }

    @Override public String getShuffleTopicFieldName() {
        return "logStore";
    }

    @Override public void destroy() {
        super.destroy();
        if (logProducer != null) {
            try {
                logProducer.close();
            } catch (InterruptedException | ProducerException e) {
                throw new RuntimeException("Sls sink close error", e);
            } finally {
                logProducer = null;
            }
        }
        ScheduleFactory.getInstance().cancel(getNameSpace() + "-" + getName() + "-sink_sts_schedule");
    }

    @Override public boolean batchInsert(List<IMessage> messages) {
        List<LogItem> logItems = new ArrayList<>();
        for (IMessage message : messages) {
            LogItem logItem = SLSUtil.createLogItem(message, logTimeFieldName);
            logItems.add(logItem);
            if (logItems.size() >= MAX_LIMIT_COUNT) {
                putLogs(project, logStore, topic, source, logItems);
                logItems = new ArrayList<>();
            }
        }
        if (logItems.size() > 0) {
            putLogs(project, logStore, topic, source, logItems);
        }

        return true;
    }

    @Override protected void createTopicIfNotExist(int splitNum) {
        lock.readLock().lock();
        try {
            if (!SLSUtil.existProject(client, project)) {
                SLSUtil.createProject(client, project);
            }

            if (!SLSUtil.existLogstore(client, project, logStore)) {
                SLSUtil.createLogstore(client, project, logStore, ttl, splitNum > 0 ? splitNum : SHARD_COUNT);
            }
        } finally {
            lock.readLock().unlock();
        }
        if (logProducer == null) {
            ProjectConfig projectConfig = null;
            try {
                projectConfig = createProjectConfig();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
            logProducer = new LogProducer(new ProducerConfig());
            logProducer.putProjectConfig(projectConfig);
        }
    }

    protected ProjectConfig createProjectConfig() throws ExecutionException {
        ProjectConfig projectConfig;
        if (isSts) {
            StsIdentity stsIdentity = stsService.getStsIdentity();
            projectConfig = new ProjectConfig(project, endPoint, stsIdentity.getAccessKeyId(), stsIdentity.getAccessKeySecret(), stsIdentity.getSecurityToken());
        } else {
            projectConfig = new ProjectConfig(project, endPoint, accessId, accessKey);
        }
        return projectConfig;
    }

    protected Client createClient() throws ExecutionException {
        Client client;
        if (isSts) {
            StsIdentity stsIdentity = stsService.getStsIdentity();
            client = new Client(endPoint, stsIdentity.getAccessKeyId(), stsIdentity.getAccessKeySecret());
            client.setSecurityToken(stsIdentity.getSecurityToken());
        } else {
            client = new Client(endPoint, accessId, accessKey);
        }
        return client;
    }

    private void putLogs(String project, String logStore, String topic, String source, List<LogItem> logItems) {
        if (project == null || logStore == null || logItems == null) {
            LOGGER.error("HandleLogTask PutLogs error, project, logStore, logItems may be null.");
            return;
        }
        if (logItems.size() > 4096) {
            LOGGER.error("HandleLogTask PutLogs error, list too much or list is 0: project:" + project + "logStore:" + logStore + "logItems size:" + logItems.size());
            return;
        }
        if (logItems.size() == 0) {
            return;
        }

        try {
            if (logProducer != null) {
                logProducer.send(getProject(), getLogStore(), topic, source, logItems, result -> {
                    if (!result.isSuccessful()) {
                        LOGGER.error(result.getErrorMessage());
                    }
                });
            } else {
                LOGGER.warn("LogProducer_Destroyed_Cannot_Send_Message");
            }
        } catch (Exception e) {
            LOGGER.error("send sls log error ", e);
        }
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

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
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

    public String getLogTimeFieldName() {
        return logTimeFieldName;
    }

    public void setLogTimeFieldName(String logTimeFieldName) {
        this.logTimeFieldName = logTimeFieldName;
    }

    public int getTtl() {
        return ttl;
    }

    public void setTtl(int ttl) {
        this.ttl = ttl;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
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

    public StsService getStsService() {
        return stsService;
    }

    public void setStsService(StsService stsService) {
        this.stsService = stsService;
    }
}