package org.apache.rocketmq.streams.script.function.impl.condition;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;

@Function
public class IFScopeFunction {

    @FunctionMethod(value = "start_if", alias = "start_if_true_false", comment = "标识case when start")
    public void startIf(IMessage message, FunctionContext context) {

    }


    @FunctionMethod(value = "end_if", alias = "end_if_true_false", comment = "标识case when end")
    public void endIf(IMessage message, FunctionContext context) {

    }
}
