package org.apache.rocketmq.streams.common.functions;

public interface GroupFunction<T, O> extends Function {

    T groupBy(O value) throws Exception;
}
