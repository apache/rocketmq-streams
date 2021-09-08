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
