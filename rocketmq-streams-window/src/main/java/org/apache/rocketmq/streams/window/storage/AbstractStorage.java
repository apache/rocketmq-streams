package org.apache.rocketmq.streams.window.storage;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractStorage implements IStorage {

    @Override
    public void init() {
    }

    @Override
    public void start() {
    }

    @Override
    public int flush(List<String> queueId) {
        return 0;
    }

    @Override
    public void clearCache(String queueId) {
    }

    //将uniqueKey放在拼接的最后一位
//    protected String buildUniqueKey(String uniqueKey, String...args) {
//        String[] temp = new String[args.length + 1];
//        for (int i = 0; i < args.length; i++) {
//            temp[i] = args[i];
//        }
//        temp[args.length] = uniqueKey;
//
//        return this.buildKey(temp);
//    }

    protected String buildKey(String... args) {
        if (args == null || args.length == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        for (String arg : args) {
            sb.append(arg);
            sb.append(IStorage.SEPARATOR);
        }

        return sb.substring(0, sb.lastIndexOf(IStorage.SEPARATOR));
    }

    protected List<String> split(String str) {
        String[] split = str.split(IStorage.SEPARATOR);
        return new ArrayList<>(Arrays.asList(split));
    }
}
