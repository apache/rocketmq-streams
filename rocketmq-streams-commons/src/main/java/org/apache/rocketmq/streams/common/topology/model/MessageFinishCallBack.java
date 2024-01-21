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
package org.apache.rocketmq.streams.common.topology.model;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

public class MessageFinishCallBack implements Future<List<JSONObject>> {
    protected Set<JSONObject> result = new HashSet<>();
    protected volatile boolean isDone = false;
    protected int sinkCount = 1;
    protected AtomicInteger finishSinkCount = new AtomicInteger(0);

    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override public boolean isCancelled() {
        return false;
    }

    @Override public boolean isDone() {
        return isDone;
    }

    @Override public List<JSONObject> get() throws InterruptedException, ExecutionException {
        if (isDone) {
            return new ArrayList<>(result);
        }
        synchronized (this) {
            this.wait();
        }
        return new ArrayList<>(result);
    }

    @Override public List<JSONObject> get(long timeout,
        TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (isDone) {
            return new ArrayList<>(result);
        }
        synchronized (this) {
            this.wait(createMilliseconds(timeout, unit));
        }
        return new ArrayList<>(result);
    }

    public synchronized void finish(List<JSONObject> result) {
        int count = finishSinkCount.incrementAndGet();
        if (result == null) {
            return;
        }
        this.result.addAll(result);
        if (count == sinkCount) {
            isDone = true;
            this.notifyAll();
        }

    }

    protected long createMilliseconds(long timeout, TimeUnit unit) {
        if (TimeUnit.MILLISECONDS.equals(unit)) {
            return timeout;
        } else if (TimeUnit.SECONDS.equals(unit)) {
            return timeout * 1000;
        } else if (TimeUnit.MINUTES.equals(unit)) {
            return timeout * 1000 * 60;
        } else if (TimeUnit.HOURS.equals(unit)) {
            return timeout * 1000 * 60 * 60;
        } else {
            throw new RuntimeException("can not support the unit " + unit + ", can use SECONDS,MINUTES,HOURS");
        }
    }

    public int getSinkCount() {
        return sinkCount;
    }

    public void setSinkCount(int sinkCount) {
        this.sinkCount = sinkCount;
    }
}
