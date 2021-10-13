package org.apache.rocketmq.streams.common.optimization.quicker;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.RandomStrUtil;
import org.omg.PortableInterceptor.INACTIVE;

/**
 * cache the expressions' result
 */
public abstract class QuickFilterResult {
    protected BitSetCache.BitSet bitSet;
    protected Map<String,Integer> expression2IndexMap;

    public QuickFilterResult(BitSetCache.BitSet bitSet, Map<String,Integer> expression2IndexMap){
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
