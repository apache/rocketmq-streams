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

import org.apache.rocketmq.streams.common.channel.source.AbstractSingleSplitSource;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;

public class MemorySource extends AbstractSingleSplitSource {

    @ConfigurableReference protected transient MemoryCache memoryCache;
    protected transient volatile boolean isClosed = false;

    public MemorySource() {

    }

    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        isClosed = false;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                Long startTime = System.currentTimeMillis();
                while (!isClosed) {
                    Object message = memoryCache.queue.poll();
                    while (message != null) {
                        doReceiveMessage(createJson(message));
                        if (memoryCache.queue != null && memoryCache.queue.size() > 10) {
                            System.out.println("memory source queues count is " + memoryCache.queue.size());
                        }
                        message = memoryCache.queue.poll();
                        if (System.currentTimeMillis() - startTime > getCheckpointTime()) {
                            sendCheckpoint(getQueueId());
                            startTime = System.currentTimeMillis();
                        }
                    }
                    if (System.currentTimeMillis() - startTime > getCheckpointTime()) {
                        sendCheckpoint(getQueueId());
                        startTime = System.currentTimeMillis();
                    }
                    sleepThread(100);
                }

            }
        });
        thread.start();
        return true;
    }

    private void sleepThread(int time) {
        try {
            Thread.sleep(time);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override protected void destroySource() {
        this.isClosed = true;
    }

    @Override
    public String getQueueId() {
        return "1";
    }

    public MemoryCache getMemoryCache() {
        return memoryCache;
    }

    public void setMemoryCache(MemoryCache memoryCache) {
        this.memoryCache = memoryCache;

    }
}
