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
package org.apache.rocketmq.streams.script.function.impl.distinct;

import org.apache.rocketmq.streams.common.cache.compress.impl.KeySet;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;

@Function
public class DistinctFunction {
    protected static int MAX_SIZE = 2000000;
    protected KeySet cache;

    public boolean containsOrPut(String... keys) {
        if (cache == null) {
            synchronized (this) {
                if (cache == null) {
                    cache = new KeySet(MAX_SIZE);
                }
            }
        }
        String key = MapKeyUtil.createKey(keys);
        boolean success = cache.contains(key);
        if (!success) {
            cache.add(key);
            /**
             * 如果超过最大值，直接归0
             */
            if (cache.getSize() > MAX_SIZE) {
                synchronized (this) {
                    if (cache.getSize() > MAX_SIZE) {
                        cache = new KeySet(MAX_SIZE);
                    }
                }
            }
        }
        return success;
    }

    @FunctionMethod(value = "distinct")
    public boolean distinct(IMessage message, AbstractContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String... fieldNames) {
        String[] values = new String[fieldNames.length];
        int i = 0;
        for (String fieldName : fieldNames) {
            values[i] = message.getMessageBody().getString(fieldName);
            i++;
        }

        boolean isContains = containsOrPut(values);
        if (isContains) {
            context.breakExecute();
        }
        return isContains;
    }
}
