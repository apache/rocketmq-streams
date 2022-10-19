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
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.streams.core.serialization.KeyValueDeserializer;
import org.apache.rocketmq.streams.core.serialization.ShuffleProtocol;

class KVJsonDeserializer<K, V> extends ShuffleProtocol implements KeyValueDeserializer<K, V> {
    private Class<K> keyType;
    private Class<V> valueType;

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Object... args) throws Throwable {
        String keyClassName = (String) args[0];
        if (!StringUtils.isEmpty(keyClassName)) {
            keyType = (Class<K>) Class.forName(keyClassName);
        }

        String valueClassName = (String) args[1];
        if (!StringUtils.isEmpty(valueClassName)) {
            valueType = (Class<V>) Class.forName(valueClassName);
        }

    }

    @Override
    public Pair<K, V> deserialize(byte[] total) throws Throwable {
        Pair<byte[], byte[]> pair = split(total);

        K key = null;
        byte[] keyBytes = pair.getObject1();
        if (keyBytes != null && keyBytes.length != 0) {
            key = JSON.parseObject(keyBytes, keyType);
        }

        V value;
        byte[] valueBytes = pair.getObject2();
        value = JSON.parseObject(valueBytes, valueType);

        return new Pair<>(key, value);
    }
}
