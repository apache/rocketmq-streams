package org.apache.rocketmq.streams.common.optimization.cachefilter;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

/**
 * group by var name
 */
public class CacheFilterGroup {
    //key: varName; value :list IOptimizationExpression
    protected String name;//mutli FilterOptimization shared cachefilter，need name
    protected String varName;
    protected List<ICacheFilter> expressionList=new ArrayList<>();
    protected BitSetCache cache;
    public CacheFilterGroup(String name,String varName,BitSetCache cache){
        this.name=name;
        this.varName=varName;
        this.cache=cache;
    }

    public void addOptimizationExpression(ICacheFilter expression){
        this.expressionList.add(expression);
    }

    public static AtomicLong totalCount=new AtomicLong(0);
    public static AtomicLong matchCount=new AtomicLong(0);
    public void execute(IMessage message, AbstractContext context){
        totalCount.incrementAndGet();
        String key= MapKeyUtil.createKey(name,message.getMessageBody().getString(varName));
        BitSetCache.BitSet bitSet = cache.get(key);
        if(bitSet==null){
            bitSet=cache.createBitSet();
            for(int i=0;i<expressionList.size();i++){
                ICacheFilter cacheFilter=expressionList.get(i);
                boolean isMatch=cacheFilter.executeOrigExpression(message,context);
                if(isMatch){
                    bitSet.set(i);
                }
                context.put(cacheFilter,isMatch);
            }
            cache.put(key,bitSet);
        }else {
            matchCount.incrementAndGet();
            for(int i=0;i<expressionList.size();i++){
                ICacheFilter cacheFilter=expressionList.get(i);
                boolean isMatch=bitSet.get(i);
                context.put(cacheFilter,isMatch);
            }
        }
//        if(totalCount.get()>0&&totalCount.get()%100==0){
//
//            System.out.println("filter rate is "+(double)matchCount.get()/(double)totalCount.get());
//        }
    }

    public int getSize(){
        return expressionList.size();
    }
}
