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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.Entity;

/**
 * @create 2021-08-06 16:21:30
 * @description
 */
public class SourceSnapShot extends Entity implements Serializable,ISplitOffset {

    private static final long serialVersionUID = 4449170945607357658L;
    public final static MetaData snapshotTable = new MetaData();

    static {
        snapshotTable.setTableName("checkpoint_snapshot");
        snapshotTable.addMetaDataField("id", "long", false);
        snapshotTable.addMetaDataField("gmt_create", "DATE", false);
        snapshotTable.addMetaDataField("gmt_modified", "DATE", false);
        snapshotTable.addMetaDataField("key", "string", false);
        snapshotTable.addMetaDataField("value", "string", false);
    }

    String key;
    String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "CheckPointSnapShot{" +
            "key='" + key + '\'' +
            ", value='" + value + '\'' +
            '}';
    }

    public JSONObject toJson() {
        JSONObject object = new JSONObject();
        object.put("key", key);
        object.put("value", value);
        return object;
    }

    @Override public String getName() {
        CheckPoint checkPoint=new CheckPoint();
        checkPoint= checkPoint.fromSnapShot(this);

        return checkPoint.getName();
    }

    @Override public String getQueueId() {
        CheckPoint checkPoint=new CheckPoint();
        checkPoint= checkPoint.fromSnapShot(this);
        return checkPoint.getQueueId();
    }

    @Override public String getOffset() {
        CheckPoint checkPoint=new CheckPoint();
        checkPoint= checkPoint.fromSnapShot(this);
        JSONObject jsonObject= JSON.parseObject(checkPoint.getOffset());
        return jsonObject.getString("offset");
    }
}
