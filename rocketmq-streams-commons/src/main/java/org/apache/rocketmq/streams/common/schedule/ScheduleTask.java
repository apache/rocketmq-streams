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
package org.apache.rocketmq.streams.common.schedule;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScheduleTask {
    protected int initialDelaySecond;
    protected int delaySecond;
    protected Runnable runnable;
    protected ExecutorService executorService;
    protected Long lastExecuteTime;
    protected IScheduleCondition scheduleCondition;
    protected AtomicBoolean isExecuting = new AtomicBoolean(false);

    public ScheduleTask(int initialDelaySecond, int delaySecond, Runnable runnable) {
        this.initialDelaySecond = initialDelaySecond;
        this.delaySecond = delaySecond;
        this.runnable = runnable;
        if (initialDelaySecond == 0) {
            lastExecuteTime = 0L;
        } else {
            lastExecuteTime = System.currentTimeMillis();
        }
        Runnable runnableProxy = new Runnable() {
            @Override
            public void run() {
                runnable.run();
                lastExecuteTime = System.currentTimeMillis();
                isExecuting.set(false);
            }
        };
        this.runnable = runnableProxy;
    }

    public ScheduleTask(IScheduleCondition scheduleCondition, Runnable runnable) {
        this.scheduleCondition = scheduleCondition;
        Runnable runnableProxy = new Runnable() {
            @Override
            public void run() {
                runnable.run();
                lastExecuteTime = System.currentTimeMillis();
                isExecuting.set(false);
            }
        };
        this.runnable = runnableProxy;
    }

    public boolean canExecute() {
        if (!isExecuting.compareAndSet(false, true)) {
            return false;
        }
        if (this.scheduleCondition != null) {
            boolean canExecute = (this.scheduleCondition.canExecute());
            if (!canExecute) {
                isExecuting.compareAndSet(true, false);
            }
            return canExecute;
        }
        if (initialDelaySecond == 0) {
            initialDelaySecond = -1;
            return true;
        }
        if (initialDelaySecond > 0 && (System.currentTimeMillis() - lastExecuteTime > initialDelaySecond)) {
            initialDelaySecond = -1;
            return true;
        }
        if (System.currentTimeMillis() - lastExecuteTime > delaySecond) {
            return true;
        }
        isExecuting.compareAndSet(true, false);
        return false;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public Runnable getRunnable() {
        return runnable;
    }

    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }
}
