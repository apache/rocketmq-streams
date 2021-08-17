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

import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.model.Entity;

/**
 * model for checkpoint，need save in store
 */
public class CheckPoint extends Entity {
    protected String sourceNamespace;
    protected String sourceName;
    protected String queueId;
    protected String offset;

    public static String loadOffset(ISource source, String splitId) {
        return null;
        //Set<String> splits=new HashSet<>();
        //splits.add(splitId);
        //Map<String,String> queueId2Offset=loadOffset(source,splits);
        //if(queueId2Offset==null||queueId2Offset.containsKey(splitId)==false){
        //    return null;
        //}
        //return queueId2Offset.get(splitId);
    }

    public String getQueueId() {
        return queueId;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public String getOffset() {
        return offset;
    }

    public void setOffset(String offset) {
        this.offset = offset;
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
}
