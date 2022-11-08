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


public class SessionWindowState<K, V> extends WindowState<K, V> {
    private long earliestTimestamp;
    private boolean closed = false;

    public SessionWindowState(K key, V value, long timestamp, long earliestTimestamp) {
        super(key, value, timestamp);
        this.earliestTimestamp = earliestTimestamp;
    }

    public long getEarliestTimestamp() {
        return earliestTimestamp;
    }

    public void setEarliestTimestamp(long earliestTimestamp) {
        this.earliestTimestamp = earliestTimestamp;
    }

    public boolean isClosed() {
        return closed;
    }

    public void setClosed(boolean closed) {
        this.closed = closed;
    }

    @Override
    public String toString() {
        return "earliestTimestamp=" + earliestTimestamp
                + "," + super.toString();

    }
}
