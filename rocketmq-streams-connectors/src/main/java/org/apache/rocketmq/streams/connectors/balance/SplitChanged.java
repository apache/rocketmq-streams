package org.apache.rocketmq.streams.connectors.balance;

import java.util.List;

import org.apache.rocketmq.streams.common.channel.split.ISplit;

public class SplitChanged {

    protected int splitCount;//变动多分片个数
    protected boolean isNewSplit;//是否新增，false是删除
    protected List<ISplit> changedSplits;
    public SplitChanged(int splitCount,boolean isNewSplit){
        this.splitCount=splitCount;
        this.isNewSplit=isNewSplit;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public void setSplitCount(int splitCount) {
        this.splitCount = splitCount;
    }

    public boolean isNewSplit() {
        return isNewSplit;
    }

    public void setNewSplit(boolean newSplit) {
        isNewSplit = newSplit;
    }

    public List<ISplit> getChangedSplits() {
        return changedSplits;
    }

    public void setChangedSplits(List<ISplit> changedSplits) {
        this.changedSplits = changedSplits;
    }
}
