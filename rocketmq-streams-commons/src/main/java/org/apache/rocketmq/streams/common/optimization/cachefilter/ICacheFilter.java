package org.apache.rocketmq.streams.common.optimization.cachefilter;

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;

/**
 * proxy a expression/function
 * @param <T>
 */
public interface ICacheFilter<T> {

    /**
     * 执行filter ，if cache return value directly，else calculte value and put cache
     * @param message
     * @param context
     * @return
     */
    boolean execute(IMessage message, AbstractContext context);


    boolean executeOrigExpression(IMessage message, AbstractContext context);
    /**
     *
     * @return expression's var name
     */
    String getVarName();

    /**
     *
     * @return ori expression
     */
    T getOriExpression();
}
