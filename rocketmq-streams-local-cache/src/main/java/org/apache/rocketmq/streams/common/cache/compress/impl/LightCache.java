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
package org.apache.rocketmq.streams.common.cache.compress.impl;

public class LightCache {
    protected IntValueKV intValueKV;
    protected int rowCount;

    public LightCache(int rowCount) {
        intValueKV = new IntValueKV(rowCount);
        this.rowCount = rowCount;
    }

    public Integer get(String key) {
        return intValueKV.get(key);
    }

    public void put(String key, Integer value) {
        if (intValueKV.getSize() > rowCount) {
            synchronized (this) {
                if (intValueKV.getSize() >= rowCount) {
                    intValueKV = new IntValueKV(rowCount);
                }
            }
        }
        intValueKV.put(key, value);
    }

    public boolean contains(String key) {
        return intValueKV.contains(key);
    }

    public int calMemory() {
        return intValueKV.calMemory();
    }

}
