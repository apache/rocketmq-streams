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
package org.apache.rocketmq.streams.core.state;

import org.apache.rocketmq.streams.core.window.WindowKey;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class RocksDBStoreTest {
    public static void main(String[] args) throws Throwable {
        RocksDBStore rocksDBStore = new RocksDBStore("test");

//        String key = "time@1668249210000@1668249195000";
//        String key2 = "ewwwwe@1668249600481@1";
        WindowKey key1 = new WindowKey("test1", "keyString1", 10l, 1l);
        WindowKey key2 = new WindowKey("test1", "keyString2", 20l, 1l);
        Object value1 = "3";
        Object value2 = "2";

        byte[] keyBytes = key2Byte(key1);
        byte[] valueBytes = Utils.object2Byte(value1);

        byte[] keyBytes2 = key2Byte(key2);
        byte[] valueBytes2 = Utils.object2Byte(value2);

        rocksDBStore.put(keyBytes2, valueBytes2);
        rocksDBStore.put(keyBytes, valueBytes);


        byte[] bytes = rocksDBStore.get(keyBytes);
        Object result = Utils.byte2Object(bytes, Object.class);
        System.out.println(result);

        byte[] bytes2 = rocksDBStore.get(keyBytes2);
        Object result2 = Utils.byte2Object(bytes2, Object.class);
        System.out.println(result2);

        WindowKey searchKey = new WindowKey("test1", "keyString1", 13l, 1l);
        String operatorName = searchKey.getOperatorName();
        List<Pair<byte[], byte[]>> pairs = rocksDBStore.searchStateLessThanWatermark(operatorName, 11l, RocksDBStoreTest::byte2WindowKey);

        System.out.println(pairs.size());
    }

    private static WindowKey byte2WindowKey(byte[] source) {
        String str = new String(source, StandardCharsets.UTF_8);
        String[] split = Utils.split(str);
        return new WindowKey(split[0], split[1], Long.parseLong(split[2]), Long.parseLong(split[3]));
    }

    private static byte[] key2Byte(WindowKey windowKey) {
        if (windowKey == null) {
            return new byte[0];
        }

        return windowKey.toString().getBytes(StandardCharsets.UTF_8);
    }
}
