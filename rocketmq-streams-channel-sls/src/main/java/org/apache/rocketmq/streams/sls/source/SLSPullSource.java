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

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.Client;
import com.aliyun.openservices.log.common.Consts;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.log.common.Shard;
import com.aliyun.openservices.log.exception.LogException;
import com.aliyun.openservices.log.request.PullLogsRequest;
import com.aliyun.openservices.log.response.GetCursorResponse;
import com.aliyun.openservices.log.response.GetCursorTimeResponse;
import com.aliyun.openservices.log.response.PullLogsResponse;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.AbstractSplitReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;
import org.apache.rocketmq.streams.sls.sink.SLSSplit;
import org.apache.rocketmq.streams.sts.StsIdentity;
import org.apache.rocketmq.streams.sts.StsService;

public class SLSPullSource extends AbstractPullSource {

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
    private LogHubProcessor logHubProcessor;
    private transient Client client;//不建议直接试用，直接试用会对sts场景造成冲突

    @Override protected boolean initConfigurable() {
        initSts();
        if (this.logHubProcessor == null) {
            this.logHubProcessor = new LogHubProcessor(this);
        }
        return super.initConfigurable();
    }

    @Override protected ISplitReader createSplitReader(ISplit<?, ?> split) {

        return new AbstractSplitReader(split) {
            private String cursor;

            @Override public void open() {
                try {
                    SLSSplit slsSplit = (SLSSplit) split;
                    GetCursorResponse cursorResponse = createClient().GetCursor(project, logStore, slsSplit.getShardId(), Consts.CursorMode.END);
                    cursor = cursorResponse.GetCursor();
                } catch (Exception e) {
                    throw new RuntimeException("获取游标错误" + "project:" + project + ";logstore:" + logStore + ";endpoint:" + endPoint, e);
                }

            }

            @Override public boolean next() {
                return true;
            }

            @Override public Iterator<PullMessage<?>> getMessage() {
                SLSSplit slsSplit = (SLSSplit) split;
                List<PullMessage<?>> pullMessages = new ArrayList<>();
                try {
                    PullLogsRequest request = new PullLogsRequest(project, logStore, slsSplit.getShardId(), pullSize, cursor);
                    PullLogsResponse response = createClient().pullLogs(request);
                    // 日志都在日志组（LogGroup）中，按照逻辑拆分即可。
                    List<LogGroupData> logGroups = response.getLogGroups();
                    AtomicInteger atomicInteger = new AtomicInteger(0);

                    String currentCursor = cursor;
                    for (LogGroupData logGroup : logGroups) {

                        List<JSONObject> msgs = logHubProcessor.doMessage(logGroup, null);
                        for (JSONObject jsonObject : msgs) {
                            PullMessage<JSONObject> pullMessage = new PullMessage<>();
                            pullMessage.setMessage(jsonObject);
                            MessageOffset messageOffset = new MessageOffset(currentCursor);
                            messageOffset.addLayerOffset(atomicInteger.incrementAndGet());
                            pullMessage.setMessageOffset(messageOffset);
                            pullMessages.add(pullMessage);
                        }
                    }

                    this.cursor = response.getNextCursor();
                } catch (Exception e) {
                    throw new RuntimeException("拉取日志错误" + "project:" + project + ";logstore:" + logStore + ";endpoint:" + endPoint, e);
                }
                return pullMessages.iterator();
            }

            @Override public void seek(String cursor) {

            }

            @Override public long getDelay() {
                SLSSplit slsSplit = (SLSSplit) split;
                try {
                    GetCursorTimeResponse response = createClient().GetCursorTime(project, logStore, slsSplit.getShardId(), cursor);
                    return System.currentTimeMillis() - response.GetCursorTime() * 1000L;
                } catch (Exception e) {
                    throw new RuntimeException("拉取日志错误" + "project:" + project + ";logstore:" + logStore + ";endpoint:" + endPoint, e);
                }
            }

            @Override public long getFetchedDelay() {
                return 0;
            }

            @Override public String getCursor() {
                return cursor;
            }
        };
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        try {
            List<Shard> shards = createClient().ListShard(project, logStore).GetShards();
            List<ISplit<?, ?>> splits = new ArrayList<>();
            for (Shard shard : shards) {
                splits.add(new SLSSplit(shard));
            }
            return splits;
        } catch (LogException e) {
            throw new RuntimeException(e);
        }

    }

    public Client createClient() {
        try {
            Client client;
            if (isSts) {
                StsIdentity stsIdentity = this.stsService.getStsIdentity();
                client = new Client(endPoint, stsIdentity.getAccessKeyId(), stsIdentity.getAccessKeySecret());
                client.setSecurityToken(stsIdentity.getSecurityToken());
                return client;
            } else {
                if (this.client != null) {
                    return this.client;
                }
                this.client = new Client(endPoint, accessId, accessKey);
                return this.client;
            }
        } catch (Exception e) {
            throw new RuntimeException("create sls client error " + "project:" + project + ";logstore:" + logStore + ";endpoint:" + endPoint, e);
        }

    }

    public boolean initSts() {
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

    @Override public void destroy() {
        super.destroy();
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

    public Client getClient() {
        return client;
    }

    public void setClient(Client client) {
        this.client = client;
    }

}
