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
package org.apache.rocketmq.streams.state.kv;

import java.util.Iterator;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.rocketmq.streams.state.LruState;
import org.junit.Assert;
import org.junit.Test;

public class TestLruState {

    @Test
    public void testBasic() {
        String content = "lru";
        int size = 100;
        final LruState<String> lruState = new LruState<>(10, "");
        //
        for (int i = 0; i < size; i++) {
            lruState.add(content);
        }
        Assert.assertEquals(size, lruState.search(content));
        Assert.assertEquals(1, lruState.count());
        lruState.remove(content);
        Assert.assertEquals(0, lruState.search(content));
        Assert.assertEquals(0, lruState.count());
        //
        for (int i = 0; i < 11; i++) {
            String value = content + "_" + i;
            for (int j = 0; j < i + 1; j++) {
                lruState.add(value);
            }
        }
        Iterator<String> iterator = lruState.iterator();
        int count = 0;
        String theValue = null;
        while (iterator.hasNext()) {
            count++;
            theValue = iterator.next();
            Assert.assertNotEquals(content + "_0", theValue);
        }
        Assert.assertEquals(content + "_1", theValue);
        Assert.assertEquals(10, count);
    }

    @Test
    public void testConcurrent() {
        String content = "lru";
        final LruState<String> lruState = new LruState<>(10, "");
        ExecutorService poolService = Executors.newFixedThreadPool(10);
        final Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < 1000; i++) {
            final int index = i;
            poolService.execute(() -> {
                lruState.add(content + random.nextInt(11));
            });
        }
        Assert.assertEquals(10, lruState.count());
    }
}


