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
package org.apache.rocketmq.streams.common.cache.compress;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.cache.HybridByteBufferTable;
import org.junit.Test;


public class HybridByteBufferTableTest {

    @Test
    public void testWrite() throws InterruptedException {
        HybridByteBufferTable table = new HybridByteBufferTable("test_hybridByteBuffer");
        Map<String, Object> row = new HashMap<>();
        byte[][] datas = null;
        double total = 0.0;
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000000; i++) {
            String key = "key" + i;
            String value = "value" + i;
//            String value = getStringValue();
            row.put("key", key);
            row.put("value", value);
            datas = table.row2Byte(row);
            table.saveRowByte(datas);
            total = total + key.length() + value.length();
        }
        table.setFinishWrite(true);
        long end = System.currentTimeMillis();
        table.executor.awaitTermination(30, TimeUnit.MINUTES);
        System.out.println("total is " + (total / (1 << 20)));
        System.out.println("time is " + (end - start));

//        table.executor.
//        while(true){
////            System.out.println("size is " + table.getQueueSize());
//            Thread.sleep(500);
//        }

    }

}
