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
package org.apache.rocketmq.streams.common.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class NameCreatorUtil {

    private transient AtomicInteger nameCreator = new AtomicInteger(10000);

    private static Map<String, NameCreatorUtil> creatorMap = new HashMap<>();

    /**
     * 每个规则一个名字生成器，expression name
     *
     * @param names
     * @return
     */
    public static NameCreatorUtil createOrGet(String... names) {
        String ruleName = MapKeyUtil.createKeyBySign("_", names);
        NameCreatorUtil nameCreator = creatorMap.get(ruleName);
        if (nameCreator != null) {
            return nameCreator;
        }
        synchronized (NameCreatorUtil.class) {
            nameCreator = creatorMap.get(ruleName);
            if (nameCreator != null) {
                return nameCreator;
            }
            nameCreator = new NameCreatorUtil();
            creatorMap.put(ruleName, nameCreator);
        }
        return nameCreator;
    }

    public static String createNewName(String... names) {
        NameCreatorUtil nameCreator = createOrGet(names);
        return nameCreator.createName(names);
    }

    protected String createName(String... namePrefix) {
        String value = MapKeyUtil.createKeyBySign("_", MapKeyUtil.createKeyBySign("_", namePrefix), nameCreator.incrementAndGet() + "");
        return value;
    }
}
