package org.apache.rocketmq.streams.common.optimization.quicker;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.HyperscanRegex;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

/**
 * optimize mutil pipeline togather
 */
public class QuickFilter {
    protected Map<String,BitSetCache> caches;

    protected List<ChainPipeline> pipelines;
    // key:varName, value:expression list
    protected Map<String,List<QuickExpression>> expressionMap=new HashMap();


    protected Map<String,List<QuickExpression>> notRegexExpressionMap=new HashMap();

    protected Map<String,HyperscanRegex> hyperscanRegexs=new HashMap<>();

    //key:varName;expression  value:index in expressionMap value list
    protected Map<String,Integer> expression2Index=new HashMap<>();

    public QuickFilter(List<ChainPipeline> pipelines){
        this.pipelines=pipelines;
    }

    /**
     * parse expressions from pipelines
     * var name need feedback source field
     */
    public void build(){
        if(expressionMap.size()==0){
            return;
        }
        parseExpressionFromPipelineList();


        Map<String,List<QuickExpression>> newExpressionMap=new HashMap();
        for(String varName:expressionMap.keySet()){
            List<QuickExpression> expressionList=expressionMap.get(varName);
            if(expressionList.size()>10){
                List<QuickExpression> notRegexExpressions=new ArrayList<>();
                HyperscanRegex hyperscanRegex=new HyperscanRegex();
                int index=0;
                for(QuickExpression expression:expressionList){
                    expression2Index.put(MapKeyUtil.createKey(varName,expression.getExpression()),index);
                    if(expression.isRegex()){
                        hyperscanRegex.addRegex(expression.getExpression(),index);
                    }else {
                        notRegexExpressions.add(expression);
                    }
                    index++;
                }

                //regex add 2 hyperscan
                if(hyperscanRegex.size()>0){
                    hyperscanRegex.compile();
                    hyperscanRegexs.put(varName,hyperscanRegex);
                }
                if(notRegexExpressions.size()>0){
                    newExpressionMap.put(varName,notRegexExpressions);
                }
                BitSetCache cache=new BitSetCache(expressionList.size(),1000000);
                caches.put(varName,cache);
            }
        }
        this.notRegexExpressionMap=newExpressionMap;
    }


    public QuickFilterResult execute(IMessage message){
        Set<Integer> matchIndexSet=new HashSet<>();
        //get result from cache
        for(String varName:this.expressionMap.keySet()){
            BitSetCache cache=caches.get(varName);
            String varValue=message.getMessageBody().getString(varName);
            BitSetCache.BitSet bitSet=cache.get(varValue);
            if(bitSet!=null){
                return new QuickFilterResult(bitSet,this.expression2Index);
            }
        }

        for(String varName:hyperscanRegexs.keySet()){
            String varValue=message.getMessageBody().getString(varName);
            HyperscanRegex hyperscanRegex=hyperscanRegexs.get(varName);
            Set<Integer> hyperscanMatchSet= hyperscanRegex.matchExpression(varValue);
            if(hyperscanMatchSet!=null&&hyperscanMatchSet.size()>0){
                matchIndexSet.addAll(hyperscanMatchSet);
            }
        }
        for(String varName:notRegexExpressionMap.keySet()){
            String varValue=message.getMessageBody().getString(varName);
            List<QuickExpression> expressionList=notRegexExpressionMap.get(varName);
            Set<Integer> otherExpressionMatchSet=new HashSet<>();
            if(expressionList!=null&&expressionList.size()>0){
                for(QuickExpression quickExpression:expressionList){
                   boolean isMatch= false;//quickExpression.executeOriExpression(varValue);
                   if(isMatch){
                       otherExpressionMatchSet.add(this.expression2Index.get(MapKeyUtil.createKey(varName,quickExpression.getExpression())));
                   }
                }
            }
            if(otherExpressionMatchSet!=null&&otherExpressionMatchSet.size()>0){
                matchIndexSet.addAll(otherExpressionMatchSet);
            }
        }

        return null;


    }

    protected void parseExpressionFromPipelineList() {
        if(this.pipelines==null||pipelines.size()==0){
            return;
        }
        for(ChainPipeline chainPipeline:pipelines){
            parseExpressionFromPipeline(chainPipeline);
        }
    }

    protected void parseExpressionFromPipeline(ChainPipeline pipeline) {
    }


    public void registeRegexExpression(QuickExpression expression){
        String varName=expression.getVarName();
        List<QuickExpression> expressionList=expressionMap.get(varName);
        if(expressionList==null){
            expressionList=new ArrayList<>();
            expressionMap.put(varName,expressionList);
        }
        expressionList.add(expression);
    }




}
