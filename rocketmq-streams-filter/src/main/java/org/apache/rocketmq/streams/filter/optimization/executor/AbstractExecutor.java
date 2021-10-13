package org.apache.rocketmq.streams.filter.optimization.executor;

import org.apache.rocketmq.streams.script.function.impl.string.RegexFunction;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public abstract class AbstractExecutor implements IScriptOptimization.IOptimizationExecutor {
    protected static RegexFunction regexFunction=new RegexFunction();// execute directly， not use reflect


}
