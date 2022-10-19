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
package org.apache.rocketmq.streams.core.serialization.deImpl;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.streams.core.serialization.KeyValueDeserializer;
import org.apache.rocketmq.streams.core.serialization.ShuffleProtocol;

import java.io.ByteArrayInputStream;

/**
 * how to decode KV
 * <pre>
 * +-----------+---------------+-----------+-------------+
 * | Int(4)    | Int(4)        | key bytes | value bytes |
 * | key length| value length  |           |             |
 * +-----------+---------------+-----------+--------- ---+
 * </pre>
 */
public class KVAvroDeserializer<K, V> extends ShuffleProtocol implements KeyValueDeserializer<K, V> {
    private DecoderFactory factory;
    private DatumReader<K> keyDatumReader;
    private DatumReader<V> valueDatumReader;


    public KVAvroDeserializer() {
        factory = DecoderFactory.get();
        keyDatumReader = new SpecificDatumReader<>();
        valueDatumReader = new SpecificDatumReader<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public void configure(Object... args) throws Throwable {
        String keyClassName = (String) args[0];
        if (!StringUtils.isEmpty(keyClassName)) {
            Class<K> keyClass = (Class<K>) Class.forName(keyClassName);
            Schema keySchema = SpecificData.get().getSchema(keyClass);
            keyDatumReader.setSchema(keySchema);
        }

        String valueClassName = (String) args[1];
        if (!StringUtils.isEmpty(valueClassName)) {
            Class<V> valueClass = (Class<V>) Class.forName(valueClassName);
            Schema valueSchema = SpecificData.get().getSchema(valueClass);
            valueDatumReader.setSchema(valueSchema);
        }
    }

    @Override
    public Pair<K, V> deserialize(byte[] total) throws Throwable {
        Pair<byte[], byte[]> pair = split(total);

        K key = null;
        byte[] keyBytes = pair.getObject1();
        if (keyBytes != null && keyBytes.length != 0) {
            try (ByteArrayInputStream bais = new ByteArrayInputStream(keyBytes)) {
                BinaryDecoder decoder = factory.binaryDecoder(bais, null);
                key = keyDatumReader.read(null, decoder);
            }
        }


        V value;
        byte[] valueBytes = pair.getObject2();
        try (ByteArrayInputStream bais = new ByteArrayInputStream(valueBytes)) {
            BinaryDecoder decoder = factory.binaryDecoder(bais, null);

            value = valueDatumReader.read(null, decoder);
        }

        return new Pair<>(key, value);
    }


}
