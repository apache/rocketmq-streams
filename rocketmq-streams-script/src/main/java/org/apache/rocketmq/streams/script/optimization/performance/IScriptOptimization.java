package org.apache.rocketmq.streams.script.optimization.performance;

import java.util.List;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.quicker.QuickFilterResult;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * optimizate script ,include compile and execute
 */
public interface IScriptOptimization {

    /**
     * compile expression list to improve performance
     * @param expressions IScriptExpression list of FunctionScript
     * @param functionScript functionScript object
     * @return the executor to to improve performance
     */
    IOptimizationCompiler compile(List<IScriptExpression> expressions, IConfigurableIdentification functionScript);




    /**
     * the executor can execute expression and return result
     */
    interface IOptimizationExecutor {
        QuickFilterResult execute(IMessage message, AbstractContext context);
    }


    /**
     * the executor can execute expression and return result
     */
    interface IOptimizationCompiler extends IOptimizationExecutor{

        /**
         * the IScriptExpression list after Optimization compile
         * @return
         */
        List<IScriptExpression> getOptimizationExpressionList();
    }

    static String getParameterValue(IScriptParamter scriptParamter) {
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
