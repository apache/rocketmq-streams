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
package org.apache.rocketmq.streams.common.channel.sinkcache;

/**
 * 自动刷新缓存的任务，开始openAutoFlush后，可以由独立线程完成数据的flush，不必显式调用
 */
public class DataSourceAutoFlushTask implements Runnable {

    private final IMessageCache<?> messageCache;
    protected transient Long lastUpdateTime;
    protected volatile int autoFlushSize = 300;
    protected volatile int autoFlushTimeGap = 1000;
    private volatile boolean isAutoFlush = false;

    public DataSourceAutoFlushTask(boolean isAutoFlush, IMessageCache<?> messageCache) {
        this.isAutoFlush = isAutoFlush;
        this.messageCache = messageCache;
        lastUpdateTime = System.currentTimeMillis();
    }

    @Override
    public void run() {
        messageCache.flush();
        lastUpdateTime = System.currentTimeMillis();
    }

    public boolean isAutoFlush() {
        return isAutoFlush;
    }

    public void setAutoFlush(boolean autoFlush) {
        isAutoFlush = autoFlush;
    }

    public int getAutoFlushSize() {
        return autoFlushSize;
    }

    public void setAutoFlushSize(int autoFlushSize) {
        this.autoFlushSize = autoFlushSize;
    }

    public Long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(Long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public int getAutoFlushTimeGap() {
        return autoFlushTimeGap;
    }

    public void setAutoFlushTimeGap(int autoFlushTimeGap) {
        this.autoFlushTimeGap = autoFlushTimeGap;
    }

}
