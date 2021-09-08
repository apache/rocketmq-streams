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
