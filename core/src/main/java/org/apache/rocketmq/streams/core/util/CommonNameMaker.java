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
package org.apache.rocketmq.streams.core.util;

import java.util.concurrent.atomic.AtomicLong;

public class CommonNameMaker {
    private static  final AtomicLong globalIndex = new AtomicLong(0);
    private final Long localIndex;
    private final String prefix;

    public CommonNameMaker(String prefix) {
        this.prefix = prefix;
        this.localIndex = globalIndex.getAndAdd(1);
    }

    public Long getLocalIndex() {
        return localIndex;
    }

    public String getPrefix() {
        return prefix;
    }
}
