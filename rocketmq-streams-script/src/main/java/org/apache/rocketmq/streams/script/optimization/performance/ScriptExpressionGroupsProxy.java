package org.apache.rocketmq.streams.script.optimization.performance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterGroup;
import org.apache.rocketmq.streams.common.optimization.cachefilter.CacheFilterManager;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class ScriptExpressionGroupsProxy extends CacheFilterManager implements IScriptExpression {
    protected List<IScriptExpression> scriptExpressions=new ArrayList<>();

    public ScriptExpressionGroupsProxy(int elementCount, int capacity) {
        super(elementCount, capacity);
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
    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        this.execute(message,context);
        for(IScriptExpression scriptExpression:scriptExpressions){
            scriptExpression.executeExpression(message,context);
        }
        return null;
    }

    @Override public List<IScriptParamter> getScriptParamters() {
        return null;
    }

    @Override public String getFunctionName() {
        return null;
    }

    @Override public String getExpressionDescription() {
        return null;
    }

    @Override public Object getScriptParamter(IMessage message, FunctionContext context) {
        return null;
    }

    @Override public String getScriptParameterStr() {
        return null;
    }

    @Override public List<String> getDependentFields() {
        return null;
    }

    @Override public Set<String> getNewFieldNames() {
        return null;
    }


}
