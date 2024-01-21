package org.apache.rocketmq.streams.dispatcher;

public interface IDispatcher<T> {

    /**
     * Start the dispatcher
     *
     * @throws Exception exception
     */
    void start() throws Exception;

    /**
     * wake up the dispatch
     *
     * @throws Exception exception
     */
    void wakeUp() throws Exception;

    /**
     * close dispatch
     *
     * @throws Exception exception
     */
    void close() throws Exception;

}
