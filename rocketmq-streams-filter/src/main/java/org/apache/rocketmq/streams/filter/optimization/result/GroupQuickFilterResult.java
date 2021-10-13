package org.apache.rocketmq.streams.filter.optimization.result;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.quicker.QuickFilterResult;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.function.expression.LikeFunction;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.function.impl.string.RegexFunction;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class GroupQuickFilterResult  extends QuickFilterResult {
    Map<String,Integer> expression2QuickFilterResultIndexMap=new HashMap<>();
    List<QuickFilterResult> results=new ArrayList<>();
    public GroupQuickFilterResult(Map<String,Integer> expression2QuickFilterResultIndexMap, List<QuickFilterResult> results){
        super(null,null);
        this.expression2QuickFilterResultIndexMap=expression2QuickFilterResultIndexMap;
        this.results=results;
    }

    @Override public Boolean isMatch(IMessage msg, Object expression) {
        String key=null;
        if(expression instanceof IScriptExpression){
            if(RegexFunction.isRegexFunction(((IScriptExpression) expression).getFunctionName())){
                IScriptExpression scriptExpression=(IScriptExpression)expression;
                String varName= IScriptOptimization.getParameterValue((IScriptParamter)scriptExpression.getScriptParamters().get(0));
                String regex=IScriptOptimization.getParameterValue((IScriptParamter)scriptExpression.getScriptParamters().get(1));
                key= MapKeyUtil.createKey(varName,scriptExpression.getFunctionName(),regex);
            }
        }else if(expression instanceof Expression){
            Expression filterExpression=(Expression)expression;
            if(LikeFunction.isLikeFunciton(filterExpression.getFunctionName())|| org.apache.rocketmq.streams.filter.function.expression.RegexFunction.isRegex(filterExpression.getFunctionName())){
                key= MapKeyUtil.createKey(filterExpression.getVarName(),filterExpression.getFunctionName(),(String)filterExpression.getValue());
            }

        }
        if(key!=null){
            Integer index= expression2QuickFilterResultIndexMap.get(key);
            if(index==null){
                return null;
            }
            QuickFilterResult quickFilterResult=results.get(index);
            return quickFilterResult.isMatch(msg,expression);
        }
        return null;
    }


}
