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
package org.apache.rocketmq.streams.core.topology.virtual;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.serialization.ShuffleProtocol;

 class KVJsonSerializer<K, V> extends ShuffleProtocol implements KeyValueSerializer<K, V> {
    @Override
    public byte[] serialize(K key, V value) throws Throwable {
        byte[] keyBytes;

        if (key == null) {
            keyBytes = new byte[0];
        } else {
            keyBytes = JSON.toJSONBytes(key, SerializerFeature.WriteClassName);
        }

        byte[] valueBytes;
        if (value == null) {
            valueBytes = new byte[0];
        } else {
            valueBytes = JSON.toJSONBytes(value, SerializerFeature.WriteClassName);
        }


        if (keyBytes.length == 0 && valueBytes.length == 0) {
            return new byte[0];
        }

        return merge(keyBytes, valueBytes);
    }
}
