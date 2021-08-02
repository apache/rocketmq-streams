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
package org.apache.rocketmq.streams.filter.operator.expression;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ExpressionPerformance implements Runnable {
    protected static List<ExpressionPerformance> expressionPerformances = new ArrayList<>();

    protected transient volatile List<String> expressionNames = new ArrayList<>();
    protected transient Map<String, ExpressionStatistic> expressionStatisticMap = new HashMap<>();
    protected transient long lastTime = System.currentTimeMillis();//最后一次的优化时间
    protected transient final List<String> values;
    private static ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(10);

    static {

        scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                for (ExpressionPerformance expressionPerformance : expressionPerformances) {
                    expressionPerformance.run();
                }
            }
        }, 10, 3, TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        List<String> newExpressionNames = new ArrayList<>();
        List<ExpressionStatistic> statistics = new ArrayList<>();
        statistics.addAll(expressionStatisticMap.values());
        Collections.sort(statistics, new Comparator<ExpressionStatistic>() {
            @Override
            public int compare(ExpressionStatistic o1, ExpressionStatistic o2) {
                return o2.count.get() - o1.count.get();
            }
        });
        for (ExpressionStatistic statistic : statistics) {
            newExpressionNames.add(statistic.name);
        }
        this.expressionNames = newExpressionNames;
        this.lastTime = System.currentTimeMillis();
    }

    protected static class ExpressionStatistic {
        protected String name;
        protected AtomicInteger count = new AtomicInteger(0);
    }

    public ExpressionPerformance(List<String> values) {
        this.expressionNames = values;
        this.values = values;
        for (String name : expressionNames) {
            ExpressionStatistic expressionStatistic = new ExpressionStatistic();
            expressionStatistic.name = name;
            expressionStatistic.count.set(0);
            expressionStatisticMap.put(name, expressionStatistic);
        }
        expressionPerformances.add(this);
    }

    public Boolean optimizate(String expressionName, Boolean value) {
        ExpressionStatistic expressionStatistic = expressionStatisticMap.get(expressionName);
        expressionStatistic.count.incrementAndGet();
        return value;
    }

    public Iterator<String> iterator() {
        if (expressionNames == null) {
            return values.iterator();
        }
        return expressionNames.iterator();//expressionNames 会做优化，会给快速失效的表达式，加权中
    }
}
