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
package org.apache.rocketmq.streams.common.threadpool;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolFactory {
    public static ExecutorService createThreadPool(int coreSize){
        ExecutorService executorService= new ThreadPoolExecutor(coreSize, coreSize,
            1000*60L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<Runnable>(),new ThreadPoolExecutor.CallerRunsPolicy());
        return executorService;
    }


    public static ExecutorService createThreadPool(int min,int max){
        ExecutorService executorService= new ThreadPoolExecutor(min, max,
            1000*60L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<Runnable>(),new ThreadPoolExecutor.CallerRunsPolicy());
        return executorService;
    }
}
