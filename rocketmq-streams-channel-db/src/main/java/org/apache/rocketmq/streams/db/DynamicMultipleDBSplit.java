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
package org.apache.rocketmq.streams.db;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

/**
 * @create 2021-07-26 16:11:29
 * @description
 */
public class DynamicMultipleDBSplit extends BasedConfigurable implements ISplit<DynamicMultipleDBSplit, String> {

    String suffix;
    String logicTableName;

    public DynamicMultipleDBSplit() {
    }

    public DynamicMultipleDBSplit(String suffix, String logicTableName) {
        this.suffix = suffix;
        this.logicTableName = logicTableName;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getLogicTableName() {
        return logicTableName;
    }

    public void setLogicTableName(String logicTableName) {
        this.logicTableName = logicTableName;
    }

    @Override
    public String getQueueId() {
        return logicTableName + "_" + suffix;
    }

    @Override
    public String getPlusQueueId() {
        throw new RuntimeException("unsupported getPlusQueueId!");
    }

    @Override
    public String getQueue() {
        return logicTableName + "_" + suffix;
    }

    @Override
    public int compareTo(DynamicMultipleDBSplit o) {
        return getQueue().compareTo(o.getQueue());
    }

    @Override
    public String toString() {
        return "DynamicMultipleDBSplit{" +
            "logicTableName='" + logicTableName + '\'' +
            ", suffix='" + suffix + '\'' +
            '}';
    }
}
