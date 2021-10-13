package org.apache.rocketmq.streams.common.context;

public interface IExpressionResultCache<E> {

    Boolean isMatch(IMessage msg,E expression);
}
