package org.apache.rocketmq.streams.connectors.balance;

import java.util.List;

import org.apache.rocketmq.streams.common.channel.split.ISplit;

public interface ISourceBalance {

    /**
     * 做负载均衡

     * @return
     */
    SplitChanged doBalance(List<ISplit> allSplits, List<ISplit> ownerSplits);

    /**
     * 从启动开始，做了多少次均衡
     * @return
     */
    int getBalanceCount();



    boolean getRemoveSplitLock();

    void unLockRemoveSplitLock();

    /**
     * lock the split and hold it util the instance is shutdown or remove split
     * @param split
     * @return
     */
    boolean holdLockSplit(ISplit split);

    /**
     * unlock split lock
     * @param split
     */
    void unlockSplit(ISplit split);


    void setSourceIdentification(String sourceIdentification);


}
