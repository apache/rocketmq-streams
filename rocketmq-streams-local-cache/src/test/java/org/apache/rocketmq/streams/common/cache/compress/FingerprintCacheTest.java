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

import org.apache.rocketmq.streams.common.cache.compress.impl.LightCache;
import org.junit.Test;

public class FingerprintCacheTest {

    @Test
    public void testFingerprintCache() {
        int cacheSize = 10000000;
        LightCache localCache = new LightCache(cacheSize);
        for (int i = 0; i < cacheSize; i++) {
            localCache.put("1.1.1." + i, i);
        }
        System.out.println(cacheSize + " cost memory is " + localCache.calMemory() + "M");
    }
}
