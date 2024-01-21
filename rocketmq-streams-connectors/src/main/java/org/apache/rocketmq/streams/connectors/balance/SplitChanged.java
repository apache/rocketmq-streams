/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.connectors.balance;

import java.util.List;
import org.apache.rocketmq.streams.common.channel.split.ISplit;

public class SplitChanged {

    protected int splitCount;//变动多分片个数
    protected boolean isNewSplit;//是否新增，false是删除
    protected List<ISplit> changedSplits;

    public SplitChanged(int splitCount, boolean isNewSplit) {
        this.splitCount = splitCount;
        this.isNewSplit = isNewSplit;
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
