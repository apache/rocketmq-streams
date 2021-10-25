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

import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
/**
 * cache the expressions' result
 */
public abstract class FilterResultCache {
    protected BitSetCache.BitSet bitSet;
    protected Map<String,Integer> expression2IndexMap;

    public FilterResultCache(BitSetCache.BitSet bitSet, Map<String,Integer> expression2IndexMap){
        this.bitSet=bitSet;
        this.expression2IndexMap=expression2IndexMap;
    }
    /**
     * if the expression in cache ,return the cache result else return null
     * @param expression regex/like/other
     * @return
     */
    public Boolean isMatch(String varName,String functionName, String expression){
        Integer index=this.expression2IndexMap.get(MapKeyUtil.createKey(varName,functionName,expression));
        if(index==null||bitSet==null){
            return null;
        }
      return bitSet.get(index);
    }

    public Map<String, Integer> getExpression2IndexMap() {
        return expression2IndexMap;
    }

    public abstract Boolean isMatch(IMessage msg,Object expression);
}
