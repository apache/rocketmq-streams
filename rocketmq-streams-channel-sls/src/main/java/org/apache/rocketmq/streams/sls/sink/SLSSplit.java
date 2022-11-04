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

import com.alibaba.fastjson.JSONObject;
import com.aliyun.openservices.log.common.Shard;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

public class SLSSplit extends BasedConfigurable implements ISplit<SLSSplit, Shard> {
    protected transient Shard shard;
    protected int shardId = 0;
    protected String shardStatus;
    protected String inclusiveBeginKey;
    protected String exclusiveEndKey;
    protected String serverIp = null;
    protected int createTime;

    @Override
    public String getQueueId() {
        return shardId + "";
    }

    @Override
    public Shard getQueue() {
        return shard;
    }

    @Override
    protected void getJsonObject(JSONObject jsonObject) {
        super.getJsonObject(jsonObject);
        Shard shard = new Shard(shardId, shardStatus, inclusiveBeginKey, exclusiveEndKey, createTime);
        shard.setServerIp(serverIp);
        this.shard = shard;
    }

    public SLSSplit(Shard shard) {
        this.shard = shard;
        this.shardId = ReflectUtil.getDeclaredField(Shard.class, shard, "shardId");
        this.shardStatus = shard.getStatus();
        this.inclusiveBeginKey = shard.getInclusiveBeginKey();
        this.exclusiveEndKey = shard.getExclusiveEndKey();
        this.serverIp = shard.getServerIp();
        this.createTime = shard.getCreateTime();
    }

    @Override
    public int compareTo(SLSSplit o) {
        return shardId - o.shardId;
    }

    public Shard getShard() {
        return shard;
    }

    public void setShard(Shard shard) {
        this.shard = shard;
    }

    public int getShardId() {
        return shardId;
    }

    public void setShardId(int shardId) {
        this.shardId = shardId;
    }

    public String getShardStatus() {
        return shardStatus;
    }

    public void setShardStatus(String shardStatus) {
        this.shardStatus = shardStatus;
    }

    public String getInclusiveBeginKey() {
        return inclusiveBeginKey;
    }

    public void setInclusiveBeginKey(String inclusiveBeginKey) {
        this.inclusiveBeginKey = inclusiveBeginKey;
    }

    public String getExclusiveEndKey() {
        return exclusiveEndKey;
    }

    public void setExclusiveEndKey(String exclusiveEndKey) {
        this.exclusiveEndKey = exclusiveEndKey;
    }

    public String getServerIp() {
        return serverIp;
    }

    public void setServerIp(String serverIp) {
        this.serverIp = serverIp;
    }

    public int getCreateTime() {
        return createTime;
    }

    public void setCreateTime(int createTime) {
        this.createTime = createTime;
    }
}
