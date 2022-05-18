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
import java.util.Random;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.apache.rocketmq.streams.configruation.model.Person;
import org.apache.rocketmq.streams.state.kv.rocksdb.RocksDBOperator;
import org.junit.Test;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

public class RocksdbTest {

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
        RocksDB rocksDB = new RocksDBOperator().getInstance();
        long start = System.currentTimeMillis();
        int size = 10000000;
        for (int i = 0; i < size; i++) {
            JSONObject msg = new JSONObject();
            msg.put("name", "chris" + i);
            msg.put("age", i);
            msg.put("address", "fddfdsfdsffddsdfsfdsfFDFDFDSDFSFDDFDSFSfddsf434234324234324234323423423423243243423443324342432" +
                "99887787878776767好iuhnkjsaddasddassaddsadsdsdssddsadasdasdsddsadsnkjjk67768767678786867fdsdsffdsfdfdsfdsfd" + i);
            rocksDB.put(msg.getString("name").getBytes(), msg.toJSONString().getBytes("UTF-8"));
        }
        Random random = new Random();
        for (int i = 0; i < size; i++) {
            long index = random.nextLong();
            rocksDB.get(("chris" + index).getBytes());
        }
        System.out.println("finish cost is " + (System.currentTimeMillis() - start));
    }

    @Test
    public void testLen() {
        System.out.println(20000000 * ("fddfdsfdsffddsdfsfdsfFDFDFDSDFSFDDFDSFSfddsffdsdsffdsfdfdsfdsfd".length() + 10));
    }

    /**
     * 100w:161471
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testSer() throws UnsupportedEncodingException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Person person = Person.createPerson("NAMESPACE" + i);
            byte[] bytes = SerializeUtil.serialize(person);
            person = SerializeUtil.deserialize(bytes);
            // assertTrue(person.getName().equals("Chris")&&person.getAge()==18&&person.getAddresses().size()==2&&person.getChildName2Age().size()==2);
        }
        System.out.println("finish cost is " + (System.currentTimeMillis() - start));

    }

    /**
     * 100w:32539
     *
     * @throws UnsupportedEncodingException
     */
    @Test
    public void testSer2() throws UnsupportedEncodingException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            Person person = Person.createPerson("NAMESPACE" + i);
            byte[] bytes = ReflectUtil.serialize(person);
            person = (Person) ReflectUtil.deserialize(bytes);
        }
        System.out.println("finish cost is " + (System.currentTimeMillis() - start));
    }

}
