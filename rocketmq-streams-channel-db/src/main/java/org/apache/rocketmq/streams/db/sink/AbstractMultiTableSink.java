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
package org.apache.rocketmq.streams.db.sink;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.functions.MultiTableSplitFunction;

public abstract class AbstractMultiTableSink extends DBSink {
    protected transient ConcurrentHashMap<String, DBSink> tableSinks = new ConcurrentHashMap();
    protected transient AtomicLong messageCount = new AtomicLong(0);
    protected transient MultiTableSplitFunction<IMessage> multiTableSplitFunction;

    public AbstractMultiTableSink(){
    }

    public AbstractMultiTableSink(String url, String userName, String password) {
        this.url = url;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public boolean batchAdd(IMessage message, ISplit split) {

        DBSink sink = getOrCreateDBSink(split.getQueueId());
        boolean success = sink.batchAdd(message, split);
        long count = messageCount.incrementAndGet();
        if (count >= getBatchSize()) {
            Set<String> queueIds = new HashSet<>();
            queueIds.add(split.getQueueId());
            flush(queueIds);
        }
        return success;
    }

    @Override
    public boolean batchAdd(IMessage message) {
        ISplit split = getSplitFromMessage(message);
        return batchAdd(message, split);
    }

    @Override
    public boolean batchSave(List<IMessage> messages) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public boolean flush(Set<String> splitIds) {
        if (splitIds == null) {
            return true;
        }
        for (String splitId : splitIds) {
            DBSink sink = getOrCreateDBSink(splitId);
            sink.flush();
        }
        return true;
    }

    @Override
    public boolean flush() {
        for (DBSink dbSink : tableSinks.values()) {
            dbSink.flush();
        }
        return true;
    }

    @Override
    public void openAutoFlush() {
        for (DBSink dbSink : tableSinks.values()) {
            dbSink.openAutoFlush();
        }
    }

    @Override
    public void closeAutoFlush() {
        for (DBSink dbSink : tableSinks.values()) {
            dbSink.closeAutoFlush();
        }
    }

    protected DBSink getOrCreateDBSink(String splitId) {
        DBSink sink = this.tableSinks.get(splitId);
        if (sink != null) {
            return sink;
        }
        sink = new DBSink();
        sink.setUrl(url);
        sink.setPassword(password);
        sink.setUserName(userName);
        sink.setTableName(createTableName(splitId));
        sink.setBatchSize(batchSize);
        sink.setJdbcDriver(this.jdbcDriver);
        sink.setMessageCache(new SingleDBSinkCache(sink));
        sink.setMultiple(true);
        sink.init();
        sink.openAutoFlush();
        DBSink existDBSink = this.tableSinks.putIfAbsent(splitId, sink);
        if (existDBSink != null) {
            return existDBSink;
        }

        return sink;
    }

    protected abstract String createTableName(String splitId);

    protected abstract ISplit getSplitFromMessage(IMessage message);

    protected class SingleDBSinkCache extends MessageCache<IMessage> {

        public SingleDBSinkCache(
            IMessageFlushCallBack<IMessage> flushCallBack) {
            super(flushCallBack);
        }

        @Override
        public int flush(Set<String> splitIds) {
            int size = super.flush(splitIds);
            messageCount.addAndGet(-size);
            return size;
        }

        @Override
        public int flush() {
            int size = super.flush();
            messageCount.addAndGet(-size);
            return size;
        }
    }

}
