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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ThreadPoolFactory {
    public static ExecutorService createThreadPool(int coreSize, String poolNamePrefix) {
        return new ThreadPoolExecutor(coreSize, coreSize, 1000 * 60L, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(), new DipperThreadFactory(poolNamePrefix), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService createThreadPool(int min, int max, String poolNamePrefix) {
        return new ThreadPoolExecutor(min, max, 1000 * 60L, TimeUnit.MILLISECONDS, new SynchronousQueue<Runnable>(), new DipperThreadFactory(poolNamePrefix), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ExecutorService createThreadPool(int min, int max, long keepAliveTime, TimeUnit timeUnit, BlockingQueue<Runnable> workQueue, String poolNamePrefix) {
        return new ThreadPoolExecutor(min, max, keepAliveTime, timeUnit, workQueue, new DipperThreadFactory(poolNamePrefix), new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public static ScheduledExecutorService createScheduledThreadPool(int coreSize, String threadNamePrefix) {
        return Executors.newScheduledThreadPool(coreSize, new DipperThreadFactory(threadNamePrefix));
    }

    public static ExecutorService createCachedThreadPool(String threadNamePrefix) {
        return Executors.newCachedThreadPool(new DipperThreadFactory(threadNamePrefix));
    }

    public static ExecutorService createFixedThreadPool(int nThreads, String threadNamePrefix) {
        return Executors.newFixedThreadPool(nThreads, new DipperThreadFactory(threadNamePrefix));
    }

    public static ExecutorService createSingleThreadExecutor(String threadNamePrefix) {
        return Executors.newSingleThreadExecutor(new DipperThreadFactory(threadNamePrefix));
    }

    public static ExecutorService createSingleScheduledThreadPool(String threadNamePrefix) {
        return Executors.newSingleThreadScheduledExecutor(new DipperThreadFactory(threadNamePrefix));
    }

    public static class DipperThreadFactory implements ThreadFactory {
        private final ThreadGroup group;
        private final AtomicInteger threadNumber = new AtomicInteger(1);
        private final String namePrefix;

        public DipperThreadFactory(String poolNamePrefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = poolNamePrefix + "-dipper-thread-";
        }

        @Override public Thread newThread(Runnable runnable) {
            Thread t = new Thread(group, runnable, namePrefix + threadNumber.getAndIncrement(), 0);
            if (t.isDaemon()) {
                t.setDaemon(false);
            }
            if (t.getPriority() != Thread.NORM_PRIORITY) {
                t.setPriority(Thread.NORM_PRIORITY);
            }
            return t;
        }
    }
}
