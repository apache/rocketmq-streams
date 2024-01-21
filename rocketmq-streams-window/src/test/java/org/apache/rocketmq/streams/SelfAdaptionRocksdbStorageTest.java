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
package org.apache.rocketmq.streams;

import com.alibaba.fastjson.JSONObject;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage;
import org.apache.rocketmq.streams.window.storage.rocksdb.RocksdbStorage;
import org.apache.rocketmq.streams.window.storage.rocksdb.SelfAdaptionRocksdbStorage;
import org.junit.Test;
import org.rocksdb.RocksDBException;

public class SelfAdaptionRocksdbStorageTest {

    @Test
    public void testSelfAdaptionRocksdbStorage() {
        SelfAdaptionRocksdbStorage storage = new SelfAdaptionRocksdbStorage();
        execute(storage);
    }

    /**
     * 1000W:179901
     * 1000w(随机读):255052ms
     * 2000w(随机读):551183ms
     * 1000W(NO Read):134853ms
     * 10000w(随机读):9263000
     *
     * @throws UnsupportedEncodingException
     * @throws RocksDBException
     */
    @Test
    public void testCPUCost() throws UnsupportedEncodingException, RocksDBException {
        RocksdbStorage rocksDB = new RocksdbStorage();
        execute(rocksDB);
    }

    protected void execute(RocksdbStorage storage) {
        long start = System.currentTimeMillis();
        int size = 200000;
        for (int i = 0; i < size; i++) {
            WindowValue windowValue = new WindowValue();

            JSONObject msg = new JSONObject();
            msg.put("name", "chris" + i);
            msg.put("age", i);
            msg.put("address", "fddfdsfdsffddsdfsfdsfFDFDFDSDFSFDDFDSFSfddsf434234324234324234323423423423243243423443324342432" +
                "99887787878776767好iuhnkjsaddasddassaddsadsdsdssddsadasdasdsddsadsnkjjk67768767678786867fdsdsffdsfdfdsfdsfd" + i);
            windowValue.setAggColumnMap(msg);
            storage.put(msg.getString("name"), windowValue);
        }
        Set<String> keys = new HashSet<>();
        keys.add("chris" + 10);
        keys.add("chris" + 100);
        keys.add("chris" + 1000);
        keys.add("chris" + 10000);
        storage.removeKeys(keys);

        WindowStorage.WindowBaseValueIterator<WindowValue> iterator = null;// storage.getByKeyPrefix("chris",WindowValue.class,true);
        int i = 0;
        while (iterator.hasNext()) {

            WindowValue windowValue = iterator.next();

//            if(!windowValue.getAggColumnResultByKey("name").equals("chris"+i)){
//                System.out.println(i);
//            }
//
//            if(!windowValue.getAggColumnResultByKey("age").equals(i)){
//                System.out.println(i);
//            }
//
//            if(!windowValue.getAggColumnResultByKey("address").equals("fddfdsfdsffddsdfsfdsfFDFDFDSDFSFDDFDSFSfddsf434234324234324234323423423423243243423443324342432" +
//                "99887787878776767好iuhnkjsaddasddassaddsadsdsdssddsadasdasdsddsadsnkjjk67768767678786867fdsdsffdsfdfdsfdsfd" + i)){
//                System.out.println(i);
//            }
            i++;
        }
        System.out.println(i);
        System.out.println("finish cost is " + (System.currentTimeMillis() - start));

    }
}
