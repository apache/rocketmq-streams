package org.apache.rocketmq.streams.common.optimization.cachefilter;

/**
 * create ICacheFilter by ori expression
 * @param <T>
 */
public interface ICacheFilterBulider<T> {

    /**
     * support cache optimize
     * @param oriExpression
     * @return
     */
    boolean support(T oriExpression);

    /**
     * create ICacheFilter by ori expression
     * @param oriExpression
     * @return
     */
    ICacheFilter<T> create(T oriExpression);

}
