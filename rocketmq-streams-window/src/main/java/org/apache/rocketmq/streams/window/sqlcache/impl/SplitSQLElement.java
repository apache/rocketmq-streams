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

public class SplitSQLElement implements ISQLElement {
    protected String queueId;
    protected String sql;

    public SplitSQLElement(String splitId, String sql) {
        this.queueId = splitId;
        this.sql = sql;
    }

    @Override public boolean isWindowInstanceSQL() {
        return false;
    }

    @Override public boolean isSplitSQL() {
        return true;
    }

    @Override public boolean isFireNotify() {
        return false;
    }

    @Override public String getQueueId() {
        return queueId;
    }

    @Override public String getWindowInstanceId() {
        throw new RuntimeException("can not support this method");
    }

    @Override public String getSQL() {
        return sql;
    }

    @Override public Integer getIndex() {
        return null;
    }

    @Override
    public void setIndex(int index) {

    }
}
