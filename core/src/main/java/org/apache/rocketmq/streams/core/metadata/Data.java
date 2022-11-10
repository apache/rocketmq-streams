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
package org.apache.rocketmq.streams.core.metadata;

import java.util.Properties;

public class Data<K, V> {
    private Properties header = new Properties();
    private K key;
    private V value;
    private long timestamp;

    public Data(K key, V value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public Data(K key, V value, long timestamp, Properties header) {
        this(key, value, timestamp);
        this.header = header;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Properties getHeader() {
        return header;
    }

    public void setHeader(Properties header) {
        this.header = header;
    }

    public <NK> Data<NK, V> key(NK key) {
        return new Data<>(key, value, timestamp, new Properties(this.header));
    }

    public <NV> Data<K, NV> value(NV value) {
        return new Data<>(key, value, timestamp, new Properties(this.header));
    }

    @Override
    public String toString() {
        return "Data{" +
                "key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
