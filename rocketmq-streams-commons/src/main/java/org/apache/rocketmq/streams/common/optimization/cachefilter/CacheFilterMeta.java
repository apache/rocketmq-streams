package org.apache.rocketmq.streams.common.optimization.cachefilter;
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
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class CacheFilterMeta {
    protected String varName;
    protected Map<String, Integer> expression2Index;
    protected BitSetCache cache;
    protected String name;
    public CacheFilterMeta(String name,String varName,Map<String, Integer> expression2Index, BitSetCache cache){
        this.varName=varName;
        this.expression2Index=expression2Index;
        this.cache=cache;
        this.name=name;
    }




    public Boolean match(String varName, String expression, IMessage message, AbstractContext context) {
        Integer index=expression2Index.get(expression);
        if(index==null){
            return null;
        }
        BitSetCache.BitSet bitSet = cache.get(createCacheKey(varName,message));
        if(bitSet==null){
            return null;
        }

        return bitSet.get(index);
    }


    public static String createCacheKey(String varName,IMessage message){
        String key= MapKeyUtil.createKey(message.getMessageBody().getString(varName));
        return key;
    }
}
