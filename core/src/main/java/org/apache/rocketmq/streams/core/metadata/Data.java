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

public class Data<K, V> {
    private String sinkTopic;
    private K key;
    private V value;
    private long timestamp;
    private long watermark;

    public Data(K key, V value, long timestamp, long watermark) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.watermark = watermark;
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

    public String getSinkTopic() {
        return sinkTopic;
    }

    public void setSinkTopic(String sinkTopic) {
        this.sinkTopic = sinkTopic;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public long getWatermark() {
        return watermark;
    }

    public void setWatermark(long watermark) {
        this.watermark = watermark;
    }

    public <NK> Data<NK,V> key(NK key) {
        return new Data<>(key, value, timestamp, watermark);
    }

    public <NV> Data<K,NV> value(NV value) {
        return new Data<>(key, value, timestamp, watermark);
    }

    @Override
    public String toString() {
        return "Context{" +
                "sinkTopic='" + sinkTopic + '\'' +
                ", key=" + key +
                ", value=" + value +
                ", timestamp=" + timestamp +
                '}';
    }
}
