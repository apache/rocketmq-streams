package org.apache.rocketmq.streams.window.storage;
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

import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractStorage implements IStorage {

    @Override
    public Future<?> load(Set<String> shuffleIds) {
        return new NullFuture();
    }

    @Override
    public int flush(List<String> queueId) {
        return 0;
    }

    @Override
    public void clearCache(String queueId) {
    }

    protected String getCurrentTimestamp() {
        long l = System.currentTimeMillis();

        return String.valueOf(l);
    }

    protected String merge(String... args) {
        if (args == null || args.length == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            sb.append(arg);
            sb.append(IStorage.SEPARATOR);
        }

        return sb.substring(0, sb.lastIndexOf(IStorage.SEPARATOR));
    }

    protected List<String> split(String str) {
        String[] split = str.split(IStorage.SEPARATOR);
        return new ArrayList<>(Arrays.asList(split));
    }

    protected long getTimestamp(Object target) {
        if (target instanceof WindowInstance) {
            return ((WindowInstance) target).getLastMaxUpdateTime();
        } else if (target instanceof WindowBaseValue) {
            return ((WindowBaseValue) target).getUpdateVersion();
        } else if (target instanceof String) {
            String time = ((String) target).split(IStorage.SEPARATOR)[0];
            return Long.parseLong(time);
        } else {
            throw new RuntimeException();
        }
    }


    static class NullFuture implements Future<Object> {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() throws InterruptedException, ExecutionException {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return null;
        }
    }
}
