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

public interface ISourceBalance {

    /**
     * 做负载均衡
     *
     * @return
     */
    SplitChanged doBalance(List<ISplit> allSplits, List<ISplit> ownerSplits);

    /**
     * 从启动开始，做了多少次均衡
     *
     * @return
     */
    int getBalanceCount();

    boolean getRemoveSplitLock();

    void unLockRemoveSplitLock();

    /**
     * lock the split and hold it util the instance is shutdown or remove split
     *
     * @param split
     * @return
     */
    boolean holdLockSplit(ISplit split);

    /**
     * unlock split lock
     *
     * @param split
     */
    void unlockSplit(ISplit split);

    void setSourceIdentification(String sourceIdentification);

}
