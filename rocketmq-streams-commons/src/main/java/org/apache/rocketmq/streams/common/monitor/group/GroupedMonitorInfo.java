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
package org.apache.rocketmq.streams.common.monitor.group;

import org.apache.rocketmq.streams.common.monitor.IMonitor;

public class GroupedMonitorInfo {

    private String name;

    private int count;

    private long max;

    private long min;

    private double avg;

    private int errorCount;

    private int slowCount;

    public void addMonitor(IMonitor monitor) {
        long costTime = monitor.getCost();
        count++;
        if (monitor.isError()) {
            errorCount++;
        }
        if (monitor.isSlow()) {
            slowCount++;
        }
        // errormsg count++
        double diffFromAvg = costTime - avg;
        avg = avg + (diffFromAvg / count);
        if (count == 1) {
            min = costTime;
            max = costTime;
        } else {
            if (costTime < min) {
                min = costTime;
            }
            if (costTime > max) {
                max = costTime;
            }
        }

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getMax() {
        return max;
    }

    public void setMax(long max) {
        this.max = max;
    }

    public long getMin() {
        return min;
    }

    public void setMin(long min) {
        this.min = min;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public int getErrorCount() {
        return errorCount;
    }

    public void setErrorCount(int errorCount) {
        this.errorCount = errorCount;
    }

    public int getSlowCount() {
        return slowCount;
    }

    public void setSlowCount(int slowCount) {
        this.slowCount = slowCount;
    }

}
