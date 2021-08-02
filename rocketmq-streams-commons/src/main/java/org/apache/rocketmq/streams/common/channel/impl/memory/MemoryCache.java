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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.InstantiationUtil;

public class MemoryCache extends BasedConfigurable {
    public static String TYPE = "memoryCache";
    protected List<String> cache = new ArrayList<>();
    protected transient ConcurrentLinkedQueue<String> queue;

    public <T> MemoryCache(T[] array) {
        this();
        for (T t : array) {
            cache.add(Base64Utils.encode(InstantiationUtil.serializeObject(t)));
        }
    }

    public MemoryCache(List<?> list) {
        this();
        for (Object t : list) {
            cache.add(Base64Utils.encode(InstantiationUtil.serializeObject(t)));
        }
    }

    public MemoryCache() {
        setType(TYPE);
    }

    @Override
    protected boolean initConfigurable() {
        queue = new ConcurrentLinkedQueue();
        for (String t : cache) {
            byte[] bytes = Base64Utils.decode(t);
            queue.offer(InstantiationUtil.deserializeObject(bytes));
        }
        return super.initConfigurable();
    }

    public List<String> getCache() {
        return cache;
    }

    public void setCache(List<String> cache) {
        this.cache = cache;
    }

    public ConcurrentLinkedQueue getQueue() {
        return queue;
    }

    public void addMessage(String msg) {
        queue.offer(msg);
    }

    public void addMessage(JSONObject msg) {
        addMessage(msg.toJSONString());
    }
}
