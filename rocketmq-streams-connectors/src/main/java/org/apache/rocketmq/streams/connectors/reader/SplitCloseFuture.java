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
package org.apache.rocketmq.streams.connectors.reader;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.rocketmq.streams.common.channel.split.ISplit;

public class SplitCloseFuture implements Future<Boolean> {

    protected ISplitReader reader;
    protected ISplit split;

    public SplitCloseFuture(ISplitReader reader, ISplit split){
        this.reader = reader;
        this.split = split;
    }
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
        return reader.isClose();
    }

    @Override
    public Boolean get() throws InterruptedException, ExecutionException {
        synchronized (reader){
            reader.wait();
        }
        return reader.isClose();
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        synchronized (reader){
            long time = timeout;
            if(unit == TimeUnit.SECONDS){
                time = time * 1000;
            }else if(unit == TimeUnit.MINUTES){
                time = time * 1000 * 60;
            }else if(unit== TimeUnit.HOURS){
                time = time * 1000 * 60 * 60;
            }else {
                throw new RuntimeException("can not support this timeout, expect less hour "+timeout+" the unit is "+unit);
            }
            reader.wait(time);
        }
        return reader.isClose();
    }

    public ISplitReader getReader() {
        return reader;
    }

    public ISplit getSplit() {
        return split;
    }
}
