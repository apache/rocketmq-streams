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

package org.apache.rocketmq.streams.window.sqlcache.impl;

import org.apache.rocketmq.streams.window.sqlcache.ISQLElement;

public class SQLElement implements ISQLElement {
    protected String queueId;
    protected String windowInstanceId;
    protected String sql;
    protected Integer index;

    public SQLElement(String splitId, String windowInstanceId, String sql) {
        this.queueId = splitId;
        this.windowInstanceId = windowInstanceId;
        this.sql = sql;
    }

    @Override public boolean isWindowInstanceSQL() {
        return true;
    }

    @Override public boolean isSplitSQL() {
        return false;
    }

    @Override public boolean isFireNotify() {
        return false;
    }

    @Override
    public String getQueueId() {
        return queueId;
    }

    @Override
    public String getWindowInstanceId() {
        return windowInstanceId;
    }

    @Override
    public String getSQL() {
        return sql;
    }

    @Override public Integer getIndex() {
        return index;
    }

    @Override
    public void setIndex(int index) {
        this.index = index;
    }

}
