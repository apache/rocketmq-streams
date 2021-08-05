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
package org.apache.rocketmq.streams.common.optimization;

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class CalculationResultCache {
    private static int MAX_COUNT = 3000000;
    private static CalculationResultCache instance = new CalculationResultCache();

    protected IntValueKV cache = new IntValueKV(MAX_COUNT);
    protected Set<String> excludeRegexs = new HashSet<>();//如果表达式重复率低，不再做统计

    private CalculationResultCache() {}

    public static CalculationResultCache getInstance() {
        return instance;
    }

    public void registeRegex(String regex, String varValue, boolean result) {
        if (cache.getSize() > MAX_COUNT) {
            cache = new IntValueKV(MAX_COUNT);
        }
        String key = MapKeyUtil.createKey(regex, varValue);
        Integer value = cache.get(key);
        if (!result) {
            value = 0;
        } else {
            if (value == null) {
                value = 0;
            }
            value++;
        }
        cache.put(key, value);

    }

    public Boolean match(String regex, String varValue) {
        String key = MapKeyUtil.createKey(regex, varValue);
        Integer value = cache.get(key);
        if (value == null) {
            return null;
        }
        return value > 0;
    }
}
