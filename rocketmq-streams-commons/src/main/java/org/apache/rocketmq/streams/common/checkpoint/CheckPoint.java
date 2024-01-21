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
package org.apache.rocketmq.streams.common.checkpoint;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

/**
 * types for checkpoint，need save in store
 */
public class CheckPoint<T> extends Entity implements ISplitOffset {

    protected String sourceNamespace;
    protected String pipelineName;
    protected String sourceName;
    protected String queueId;
    protected String topic;

    protected T data;

    protected JSONObject content;

    @Override public String getName() {
        return MapKeyUtil.createKey(sourceNamespace, sourceName);
    }

    @Override
    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    @Override public String getOffset() {
        return data.toString();
    }

    public String getSourceNamespace() {
        return sourceNamespace;
    }

    public void setSourceNamespace(String sourceNamespace) {
        this.sourceNamespace = sourceNamespace;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public JSONObject getContent() {
        return content;
    }

    public void setContent(JSONObject content) {
        this.content = content;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public SourceSnapShot toSnapShot() {
        SourceSnapShot snapShot = new SourceSnapShot();
        snapShot.setGmtCreate(gmtCreate);
        snapShot.setGmtModified(gmtModified);
        snapShot.setKey(CheckPointManager.createCheckPointKey(sourceName, queueId));
        if (content == null) {
            content = new JSONObject();
        }
        content.put("offset", data);

        snapShot.setValue(content.toString());
        return snapShot;

    }

    public CheckPoint fromSnapShot(SourceSnapShot sourceSnapShot) {

        if (sourceSnapShot == null) {
            return null;
        }

        String key = sourceSnapShot.getKey();
        String value = sourceSnapShot.getValue();
        CheckPoint<String> checkPoint = new CheckPoint<>();
        String[] tmp1 = CheckPointManager.parseCheckPointKey(key);
        String[] tmp2 = tmp1[0].split(";");
        checkPoint.setSourceNamespace(tmp2[0]);
        checkPoint.setPipelineName(tmp2[1]);
        checkPoint.setSourceName(tmp2[2]);
        checkPoint.setQueueId(tmp1[1]);
        checkPoint.setData(value);
        checkPoint.setGmtCreate(sourceSnapShot.getGmtCreate());
        checkPoint.setGmtModified(sourceSnapShot.getGmtModified());
        return checkPoint;

    }
}