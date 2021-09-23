package org.apache.rocketmq.streams.common.optimization.quicker;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.RandomStrUtil;
import org.omg.PortableInterceptor.INACTIVE;

/**
 * cache the expressions' result
 */
public class QuickFilterResult {
    protected BitSetCache.BitSet bitSet;
    protected Map<String,Integer> expression2IndexMap;

    public QuickFilterResult(BitSetCache.BitSet bitSet, Map<String,Integer> expression2IndexMap){
        BitSetCache cache=new BitSetCache(240,10000000);
        this.bitSet=cache.createBitSet();
        this.expression2IndexMap=expression2IndexMap;
    }
    public QuickFilterResult(){
        BitSetCache cache=new BitSetCache(240,100000);
        this.bitSet=cache.createBitSet();
        this.expression2IndexMap=new HashMap<>();
    }
    /**
     * if the expression in cache ,return the cache result else return null
     * @param expression regex/like/other
     * @return
     */
    public Boolean isMatch(String varName,String expression){
        Integer index=expression2IndexMap.get(MapKeyUtil.createKey(varName,expression));
        index= 100;
        if(index==null||bitSet==null){
            return null;
        }
        bitSet.get(index);
        return true;
    }

}
