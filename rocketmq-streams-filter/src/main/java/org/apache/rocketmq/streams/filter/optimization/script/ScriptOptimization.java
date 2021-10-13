package org.apache.rocketmq.streams.filter.optimization.script;

import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.optimization.casewhen.CaseWhenBuilder;
import org.apache.rocketmq.streams.filter.optimization.executor.GroupByVarExecutor;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

@AutoService(IScriptOptimization.class)
public class ScriptOptimization  implements IScriptOptimization{

    public static interface IExpressionExecutor<T>{
        boolean execute(IMessage message, AbstractContext context);
    }


    @Override public IOptimizationCompiler compile(List<IScriptExpression> expressions, IConfigurableIdentification configurableIdentification) {
        if(expressions!=null){
            GroupByVarExecutor groupByVarExecutor=new GroupByVarExecutor(configurableIdentification.getNameSpace(),configurableIdentification.getConfigureName(),expressions);

            expressions=groupByVarExecutor.compile();
            expressions=CaseWhenBuilder.compile(configurableIdentification.getNameSpace(),configurableIdentification.getConfigureName(),expressions);
            groupByVarExecutor.setScriptExpressions(expressions);
            return groupByVarExecutor;
        }
        return null;
    }
}
