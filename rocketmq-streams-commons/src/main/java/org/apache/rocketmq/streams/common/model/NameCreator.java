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
package org.apache.rocketmq.streams.common.model;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class NameCreator {
    private final transient AtomicInteger nameCreator = new AtomicInteger(100000000);

    private final Map<String, NameCreator> creatorMap = new HashMap<>();

    public static String getFirstPrefix(String name, String... types) {
        for (String type : types) {
            int index = name.indexOf("_" + type);
            if (index != -1) {
                return name.substring(0, index);
            }
        }
        return name;
    }

    /**
     * 每个规则一个名字生成器，expression name
     *
     * @param names 名称
     * @return NameCreator实例
     */
    public NameCreator createOrGet(String... names) {
        String ruleName = MapKeyUtil.createKeyBySign("_", names);
        NameCreator nameCreator = creatorMap.get(ruleName);
        if (nameCreator != null) {
            return nameCreator;
        }
        synchronized (NameCreator.class) {
            nameCreator = creatorMap.get(ruleName);
            if (nameCreator != null) {
                return nameCreator;
            }
            nameCreator = new NameCreator();
            creatorMap.put(ruleName, nameCreator);
        }
        return nameCreator;
    }

    public String createName(String... namePrefix) {
        return MapKeyUtil.createKeyBySign("_", MapKeyUtil.createKeyBySign("_", namePrefix), String.valueOf(nameCreator.incrementAndGet()));
    }
}
