package org.apache.rocketmq.streams.common.optimization.cachefilter;

import java.util.BitSet;
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
