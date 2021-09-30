package org.apache.rocketmq.streams.connectors.balance.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.connectors.balance.IBalanceTask;
import org.apache.rocketmq.streams.connectors.balance.ISourceBalance;
import org.apache.rocketmq.streams.connectors.balance.SplitChanged;

import java.util.ArrayList;
import java.util.List;

/**
 * @description
 */
//public class DefaultBalanceTask implements IBalanceTask {
//
//    static Log logger = LogFactory.getLog(DefaultBalanceTask.class);
//
//    ISourceBalance iSourceBalance;
//
//    public DefaultBalanceTask(ISourceBalance iSourceBalance){
//        this.iSourceBalance = iSourceBalance;
//    }
//    @Override
//    public void run() {
//        logger.info("balance running..... current splits is " + ownerSplits);
//        List<ISplit> allSplits = fetchAllSplits();
//        SplitChanged splitChanged = balance.doBalance(allSplits, new ArrayList(ownerSplits.values()));
//        doSplitChanged(splitChanged);
//    }
//}
