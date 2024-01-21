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
package org.apache.rocketmq.streams.common.channel.impl;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.source.AbstractSingleSplitSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description for test checkpoint
 */
public class CollectionSource extends AbstractSingleSplitSource implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CollectionSource.class);
    private static final int checkpointIntervalMs = 10 * 1000;
    transient ConcurrentLinkedQueue<JSONObject> queue = new ConcurrentLinkedQueue<>();
    transient AtomicLong offset = new AtomicLong(0);
    long maxOffset;
    //must be json string
    List<String> elements;
    long lastCheckpointTime = System.currentTimeMillis();

    transient volatile long currentOffset;

    public CollectionSource() {
        elements = new ArrayList<>();
    }

    private final boolean isInterrupted() {
        return currentOffset == maxOffset;
    }

    private synchronized JSONObject consume() {
        while (queue.isEmpty()) {
            try {
                logger.info("queue is empty, sleep 50ms.");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        currentOffset = offset.incrementAndGet();
        try {
            Thread.sleep(10);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return queue.poll();
    }

    public CollectionSource addAll(JSONObject... elements) {
        maxOffset = elements.length;
        if (this.elements == null) {
            this.elements = new ArrayList<>();
        }
        for (JSONObject e : elements) {
            this.elements.add(e.toJSONString());
        }
        return this;
    }

    @Override
    public boolean initConfigurable() {
        elements.forEach(e -> queue.offer(JSONObject.parseObject(e)));
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        new Thread(new Runnable() {

            @Override
            public void run() {
                while (isInterrupted() == false) {
                    JSONObject message = consume();
                    boolean isCheckPoint = false;
                    long cur = System.currentTimeMillis();
                    if (cur - lastCheckpointTime > checkpointIntervalMs) {
                        System.out.println("start checkping....");
                        isCheckPoint = true;
                        lastCheckpointTime = cur;
                    }
                    doReceiveMessage(message, isCheckPoint, "1", String.valueOf(currentOffset));

                }
            }
        }).start();

        return true;
    }

    @Override protected void destroySource() {

    }

    public long getMaxOffset() {
        return maxOffset;
    }

    public void setMaxOffset(long maxOffset) {
        this.maxOffset = maxOffset;
    }

    public List<String> getElements() {
        return elements;
    }

    public void setElements(List elements) {
        this.elements = elements;
    }

    public long getLastCheckpointTime() {
        return lastCheckpointTime;
    }

    public void setLastCheckpointTime(long lastCheckpointTime) {
        this.lastCheckpointTime = lastCheckpointTime;
    }
}
