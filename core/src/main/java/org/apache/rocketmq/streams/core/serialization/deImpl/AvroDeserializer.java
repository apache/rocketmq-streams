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

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.rocketmq.streams.core.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;

public class AvroDeserializer<T> implements Deserializer<T> {
    private DecoderFactory factory;
    private DatumReader<T> datumReader;

    public AvroDeserializer() {
        factory = DecoderFactory.get();
        datumReader = new SpecificDatumReader<>();
    }

    @Override
    public T deserialize(byte[] data) throws Throwable {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        BinaryDecoder decoder = factory.binaryDecoder(bais, null);
        ByteBuffer buffer = ByteBuffer.allocate(16);

        try {
            decoder.readBytes(buffer);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return datumReader.read(null, decoder);
    }
}
