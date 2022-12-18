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
package org.apache.rocketmq.streams.core.runtime.operators;

import java.util.concurrent.TimeUnit;

import static io.netty.util.internal.ObjectUtil.checkNotNull;

public class Time {
    private final TimeUnit unit;
    private final long size;


    private Time(long size, TimeUnit unit) {
        this.unit = checkNotNull(unit, "time unit may not be null");
        this.size = size;
    }

    public TimeUnit getUnit() {
        return unit;
    }


    public long getSize() {
        return size;
    }


    public long toMilliseconds() {
        return unit.toMillis(size);
    }


    public static Time of(long size, TimeUnit unit) {
        return new Time(size, unit);
    }

    public static Time milliseconds(long milliseconds) {
        return of(milliseconds, TimeUnit.MILLISECONDS);
    }

    public static Time seconds(long seconds) {
        return of(seconds, TimeUnit.SECONDS);
    }

    public static Time minutes(long minutes) {
        return of(minutes, TimeUnit.MINUTES);
    }

    public static Time hours(long hours) {
        return of(hours, TimeUnit.HOURS);
    }

    public static Time days(long days) {
        return of(days, TimeUnit.DAYS);
    }

    public long toMillSecond() {
        return this.unit.toMillis(this.size);
    }
}
