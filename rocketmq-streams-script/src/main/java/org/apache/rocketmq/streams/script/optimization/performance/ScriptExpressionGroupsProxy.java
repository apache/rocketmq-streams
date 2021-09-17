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

package org.apache.rocketmq.streams.script.optimization.performance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterGroup;
import org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterManager;
import org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterMeta;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class ScriptExpressionGroupsProxy extends CacheFilterManager  {
    protected List<IScriptExpression> scriptExpressions=new ArrayList<>();


    public ScriptExpressionGroupsProxy(int elementCount, int capacity) {
        super(elementCount, capacity);
    }

    public static Boolean inFilterCache(String varName, String expression, IMessage message, AbstractContext context) {
        CacheFilterMeta cacheFilterMeta=getCacheFilterMeta(varName,context);
        if(cacheFilterMeta==null){
            return null;
        }
        return cacheFilterMeta.match(varName,expression,message,context);
    }

    public static void setFilterCache(String varName, String expression, IMessage message, AbstractContext context){

    }

    private static CacheFilterMeta getCacheFilterMeta(String varName, AbstractContext context) {
        String key=createCacheKey(varName);
        return (CacheFilterMeta) context.get(key);
    }


    public void removeLessCount() {
        Map<String, CacheFilterGroup> newFilterOptimizationMap=new HashMap<>();
        for(String varName:this.filterOptimizationMap.keySet()){
            CacheFilterGroup cacheFilterGroup =this.filterOptimizationMap.get(varName);
            if(cacheFilterGroup.getSize()>5){
                newFilterOptimizationMap.put(varName,cacheFilterGroup);
            }
        }
        this.filterOptimizationMap=newFilterOptimizationMap;
    }
    public void addScriptExpression(IScriptExpression scriptExpression){
        this.scriptExpressions.add(scriptExpression);
    }

}
