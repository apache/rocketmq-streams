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
import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;
import com.aliyun.openservices.loghub.client.DefaultLogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.ILogHubCheckPointTracker;
import com.aliyun.openservices.loghub.client.LogHubClientAdapter;
import com.aliyun.openservices.loghub.client.interfaces.ILogHubProcessor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

public class LogHubProcessor implements ILogHubProcessor {
    private int mShardId;
    // 记录上次持久化 check point 的时间
    private long mLastCheckTime = 0;

    @Override
    public void initialize(int shardId) {
        mShardId = shardId;
    }

    private final SLSSource channel;

    public LogHubProcessor(SLSSource channel) {
        this.channel = channel;
    }

    // 消费数据的主逻辑
    @Override
    public String process(List<LogGroupData> logGroups, ILogHubCheckPointTracker checkPointTracker) {
        Long firstOffset = null;
        int shardId = ReflectUtil.getDeclaredField(checkPointTracker, "shardID");
        String checkpoint = ReflectUtil.getDeclaredField(checkPointTracker, "cursor");
        if (logGroups.size() > 0) {
            this.mShardId = shardId;
            firstOffset = createOffset(checkpoint);
            AtomicInteger atomicInteger = new AtomicInteger(0);
            for (LogGroupData logGroup : logGroups) {

                List<JSONObject> msgs = doMessage(logGroup, checkPointTracker);
                for (JSONObject jsonObject : msgs) {
                    Message msg = channel.createMessage(jsonObject, shardId + "", firstOffset + "", false);
                    msg.getHeader().addLayerOffset(atomicInteger.incrementAndGet());
                    //shardOffset.subOffset.add(msg.getHeader().getOffset());
                    msg.getHeader().setOffsetIsLong(true);
                    channel.executeMessage(msg);
                }
            }
        }

        long curTime = System.currentTimeMillis();
        //每200ms调用一次
        // 每隔 60 秒，写一次 check point 到服务端，如果 60 秒内，worker crash，
        // 新启动的 worker 会从上一个 checkpoint 其消费数据，有可能有重复数据
        try {
            if (curTime - mLastCheckTime > channel.getCheckpointTime()) {
                mLastCheckTime = curTime;
                channel.sendCheckpoint(shardId + "");
                checkPointTracker.saveCheckPoint(true);
            } else {
                checkPointTracker.saveCheckPoint(false);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 返回空表示正常处理数据， 如果需要回滚到上个 check point 的点进行重试的话，可以 return checkPointTracker.getCheckpoint()
        return null;
    }

    protected Long createOffset(String checkpoint) {
        try {
            byte[] bytes = (Base64Utils.decode(checkpoint));
            String offsetStr = new String(bytes);
            return Long.valueOf(offsetStr);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // 当 worker 退出的时候，会调用该函数，用户可以在此处做些清理工作。
    @Override
    public void shutdown(ILogHubCheckPointTracker checkPointTracker) {
        //Integer shardId = ReflectUtil.getBeanFieldOrJsonValue(checkPointTracker, "shardID");
        //channel.sendCheckpoint(shardId+"");
        Set<String> shards = new HashSet<>();
        shards.add(mShardId + "");
        this.channel.removeSplit(shards);
    }

    public List<JSONObject> doMessage(LogGroupData logGroup, ILogHubCheckPointTracker checkPointTracker) {
        List<JSONObject> messsages = new ArrayList<>();
        FastLogGroup flg = logGroup.GetFastLogGroup();
        for (int lIdx = 0; lIdx < flg.getLogsCount(); lIdx++) {
            String message = null;
            FastLog log = flg.getLogs(lIdx);
            if (!channel.getJsonData()) {
                if (log.getContentsCount() == 0) {
                    continue;
                }
                FastLogContent content = log.getContents(0);
                message = content.getValue();
            } else {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("logTime", log.getTime());
                for (int cIdx = 0; cIdx < log.getContentsCount(); cIdx++) {
                    FastLogContent content = log.getContents(cIdx);
                    jsonObject.put(content.getKey(), content.getValue());
                }

                //为了兼容用户自定义类型，不要删除这句
                message = jsonObject.toJSONString();
            }

            Map<String, Object> property = createHeaderProperty(log, flg, checkPointTracker);
            JSONObject msg = channel.create(message, property);
            messsages.add(msg);
        }
        return messsages;
    }

    private Map<String, Object> createHeaderProperty(FastLog log, FastLogGroup flg, ILogHubCheckPointTracker checkPointTracker) {
        try {

            if (channel.getHeaderFieldNames() != null) {
                Map<String, Object> property = new HashMap<>();
                property = new HashMap<>();
                property.put("__source__", flg.getSource());
                property.put("__topic__", flg.getTopic());
                property.put("__timestamp__", log.getTime());
                if (checkPointTracker instanceof DefaultLogHubCheckPointTracker) {
                    DefaultLogHubCheckPointTracker defaultLogHubCheckPointTracker = (DefaultLogHubCheckPointTracker) checkPointTracker;
                    LogHubClientAdapter logHubClientAdapte = ReflectUtil.getDeclaredField(defaultLogHubCheckPointTracker, "loghubClient");
                    if (logHubClientAdapte != null) {
                        Client client = ReflectUtil.getDeclaredField(logHubClientAdapte, "client");
                        if (client != null) {
                            String hostName = ReflectUtil.getDeclaredField(client, "hostName");
                            property.put("__hostname__", hostName);
                        }
                    }
                }
                return property;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}