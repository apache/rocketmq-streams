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
package org.apache.rocketmq.streams.core.serialization.serImpl;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.serialization.ShuffleProtocol;

import java.io.ByteArrayOutputStream;


/**
 * how to encode KV
 * <pre>
 * +-----------+---------------+-----------+-------------+
 * | Int(4)    | Int(4)        | key bytes | value bytes |
 * | key length| value length  |           |             |
 * +-----------+---------------+-----------+-------------+
 * </pre>
 */
public class KVAvroSerializer<K, V> extends ShuffleProtocol implements KeyValueSerializer<K, V> {
    private Schema keySchema;
    private Schema valueSchema;

    @Override
    @SuppressWarnings("unchecked")
    public byte[] serialize(K key, V value) throws Throwable {
        byte[] keyBytes;

        if (key == null) {
            keyBytes = new byte[0];
        } else {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

                if (keySchema == null) {
                    Class<K> keyClass = (Class<K>) key.getClass();
                    this.keySchema = SpecificData.get().getSchema(keyClass);
                }

                SpecificDatumWriter<K> keyWriter = new SpecificDatumWriter<>(keySchema);

                keyWriter.write(key, encoder);
                encoder.flush();

                keyBytes = out.toByteArray();
            }
        }


        byte[] valueBytes;
        if (value == null) {
            valueBytes = new byte[0];
        } else {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);

                if (valueSchema == null) {
                    Class<V> valueClass = (Class<V>) value.getClass();
                    this.valueSchema = SpecificData.get().getSchema(valueClass);
                }

                SpecificDatumWriter<V> valueWriter = new SpecificDatumWriter<>(valueSchema);

                valueWriter.write(value, encoder);
                encoder.flush();

                valueBytes = out.toByteArray();
            }
        }

        if (keyBytes.length == 0 && valueBytes.length == 0) {
            return null;
        }

        return merge(keyBytes, valueBytes);
    }
}
