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
package org.apache.rocketmq.streams.common.channel.impl.memory;

import org.apache.rocketmq.streams.common.channel.source.AbstractBatchSource;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;

public class MemorySource extends AbstractBatchSource implements IAfterConfigurableRefreshListener {

    protected String cacheName;
    protected transient MemoryCache memoryCache;

    public MemorySource() {

    }

    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        boolean success = super.startSource();
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    while (true) {
                        Object message = memoryCache.queue.poll();
                        while (message != null) {
                            doReceiveMessage(createJson(message));
                            message = memoryCache.queue.poll();
                        }
                        sendCheckpoint(getQueueId());
                        Thread.sleep(1000);
                    }

                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        });
        thread.start();
        return true;
    }

    @Override
    public String getQueueId() {
        return "1";
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        memoryCache = configurableService.queryConfigurable(MemoryCache.TYPE, cacheName);
    }

    public void setMemoryCache(MemoryCache memoryCache) {
        this.memoryCache = memoryCache;
        setCacheName(memoryCache.getConfigureName());

    }

    public MemoryCache getMemoryCache() {
        return memoryCache;
    }
}
