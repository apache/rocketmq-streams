package org.apache.rocketmq.streams.filter;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.builder.RuleBuilder;
import org.apache.rocketmq.streams.filter.function.script.CaseFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ICaseWhenCompile;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@AutoService(ICaseWhenCompile.class)
public class CaseWhenCompile implements ICaseWhenCompile {
    @Override public List<? extends AbstractRule> compile(GroupScriptExpression groupScriptExpression) {
        List<Rule> rules=new ArrayList<>();
        IScriptExpression thenScriptExpression = groupScriptExpression.getThenExpresssions().get(0);
        Rule rule=createRule(groupScriptExpression.getIfExpresssion(),thenScriptExpression);
        rules.add(rule);
        if(groupScriptExpression.getElseIfExpressions()!=null){
            for(GroupScriptExpression subGroupScriptExpression:groupScriptExpression.getElseIfExpressions()){
                List<Rule> ruleList= (List<Rule>) compile(subGroupScriptExpression);
                if(ruleList!=null){
                    rules.addAll(ruleList);
                }
            }
        }
        return rules;
    }

    protected Rule createRule(IScriptExpression ifExpression,IScriptExpression thenScriptExpression){
        if(!CaseFunction.isCaseFunction(ifExpression.getFunctionName())){
            throw new RuntimeException("can not support compile groupScriptExpression ,only support CaseFunction, real is "+ifExpression.getFunctionName());
        }
        IScriptParamter scriptParamter= (IScriptParamter) ifExpression.getScriptParamters().get(0);
        String expression=getParameterValue(scriptParamter);
        //Rule rule= ExpressionBuilder.createRule("tmp","tmp",expression);
        RuleBuilder ruleBuilder=new RuleBuilder("tmp","tmp",expression);
        ruleBuilder.addCaseWhenAction(thenScriptExpression);
        Rule rule=ruleBuilder.generateRule();
        return rule;
    }

    protected String getParameterValue(IScriptParamter scriptParamter) {
        if (!ScriptParameter.class.isInstance(scriptParamter)) {
            return null;
        }
        ScriptParameter parameter = (ScriptParameter) scriptParamter;
        if (parameter.getRigthVarName() != null) {
            return null;
        }
        return FunctionUtils.getConstant(parameter.getLeftVarName());
    }
}
