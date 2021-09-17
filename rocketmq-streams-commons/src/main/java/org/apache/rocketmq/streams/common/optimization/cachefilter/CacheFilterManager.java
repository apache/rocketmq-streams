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

package org.apache.rocketmq.streams.common.optimization.cachefilter;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

import static org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterGroup.FILTER_CACHE_KEY;

public class CacheFilterManager {
    protected BitSetCache cache;

    protected Map<String, CacheFilterGroup> filterOptimizationMap=new HashMap<>();
    public CacheFilterManager(int elementCount,int capacity){
        cache=new BitSetCache(elementCount,capacity);
    }

    public CacheFilterManager( BitSetCache cache){
        this.cache=cache;
    }

    public void addOptimizationExpression(String name, ICacheFilter expression){
        String varName=expression.getVarName();
        CacheFilterGroup filterOptimization=filterOptimizationMap.get(varName);
        if(filterOptimization==null){
            filterOptimization=new CacheFilterGroup(name,varName,this.cache);
            filterOptimizationMap.put(varName,filterOptimization);
        }
        filterOptimization.addOptimizationExpression(expression);
    }

    public void executeExpression(IMessage message, AbstractContext context) {
        for(CacheFilterGroup  filterOptimization: filterOptimizationMap.values()){
            filterOptimization.execute(message,context);

        }
    }


    protected static String createCacheKey(String varName){
        return MapKeyUtil.createKey(FILTER_CACHE_KEY,varName);
    }




}
