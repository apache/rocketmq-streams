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
package org.apache.rocketmq.streams.connectors.balance.impl;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.connectors.balance.AbstractBalance;
import org.apache.rocketmq.streams.connectors.balance.ISourceBalance;
import org.apache.rocketmq.streams.connectors.source.SourceInstance;
import org.apache.rocketmq.streams.lease.LeaseComponent;
import org.apache.rocketmq.streams.lease.model.LeaseInfo;
import org.apache.rocketmq.streams.lease.service.ILeaseService;

@AutoService(ISourceBalance.class)
@ServiceName(LeaseBalanceImpl.DB_BALANCE_NAME)
public class LeaseBalanceImpl extends AbstractBalance {

    private static final Log logger = LogFactory.getLog(LeaseBalanceImpl.class);

    public static final String DB_BALANCE_NAME = "db_balance";
    private static final String REMOVE_SPLIT_LOCK_NAME = "lock_remove_split";
    private static final String SOURCE_LOCK_PREFIX = "SOURCE_";
    private static final String SPLIT_LOCK_PREFIX = "SPLIT_";
    protected transient LeaseComponent leaseComponent = LeaseComponent.getInstance();
    protected transient String sourceIdentification;

    protected int lockTimeSecond = 5;

    public LeaseBalanceImpl(String sourceIdentification) {

        this.sourceIdentification = sourceIdentification;
    }

    public LeaseBalanceImpl() {

    }

    @Override
    protected List<ISplit> fetchWorkingSplits(List<ISplit> allSplits) {
        List<LeaseInfo> leaseInfos = leaseComponent.getService().queryLockedInstanceByNamePrefix(SPLIT_LOCK_PREFIX + this.sourceIdentification, null);
        logger.info(String.format("lease SPLIT_LOCK_PREFIX is %s, sourceIdentification is %s. ", SPLIT_LOCK_PREFIX, sourceIdentification));
        if (leaseInfos == null) {
            return new ArrayList<>();
        }

        Map<String, ISplit> allSplitMap = new HashMap<>();
        for (ISplit split : allSplits) {
            allSplitMap.put(split.getQueueId(), split);
        }
        List<ISplit> splits = new ArrayList<>();
        for (LeaseInfo leaseInfo : leaseInfos) {
            String leaseName = leaseInfo.getLeaseName();
            String splitId = MapKeyUtil.getLast(leaseName);
            splits.add(allSplitMap.get(splitId));
        }
        logger.info(String.format("working split is %s", Arrays.toString(splits.toArray())));
        return splits;
    }

    @Override
    protected List<SourceInstance> fetchSourceInstances() {
        List<LeaseInfo> leaseInfos = leaseComponent.getService().queryLockedInstanceByNamePrefix(SOURCE_LOCK_PREFIX + sourceIdentification, null);
        if (leaseInfos == null) {
            return new ArrayList<>();
        }
        List<SourceInstance> sourceInstances = new ArrayList<>();
        for (LeaseInfo leaseInfo : leaseInfos) {
            String leaseName = leaseInfo.getLeaseName();
            sourceInstances.add(new SourceInstance(leaseName));
        }
        return sourceInstances;
    }

    @Override
    protected boolean holdLockSourceInstance() {
        return holdLock(SOURCE_LOCK_PREFIX + sourceIdentification, RuntimeUtil.getDipperInstanceId());
    }

    @Override
    protected void unlockSourceInstance() {
        leaseComponent.getService().unlock(SOURCE_LOCK_PREFIX + sourceIdentification, RuntimeUtil.getDipperInstanceId());
    }

    @Override
    public boolean holdLockSplit(ISplit split) {
        return holdLock(SPLIT_LOCK_PREFIX + this.sourceIdentification, split.getQueueId());
    }

    @Override
    public void unlockSplit(ISplit split) {
        leaseComponent.getService().unlock(SPLIT_LOCK_PREFIX + this.sourceIdentification, split.getQueueId());

    }

    @Override
    public boolean getRemoveSplitLock() {
        return holdLock(this.sourceIdentification, REMOVE_SPLIT_LOCK_NAME);
    }

    @Override
    public void unLockRemoveSplitLock() {
        leaseComponent.getService().unlock(this.sourceIdentification, REMOVE_SPLIT_LOCK_NAME);
    }

    public String getSourceIdentification() {
        return sourceIdentification;
    }

    @Override
    public void setSourceIdentification(String sourceIdentification) {
        this.sourceIdentification = sourceIdentification;
    }

    protected boolean holdLock(String name, String lockName) {
        ILeaseService leaseService = leaseComponent.getService();
        boolean success = leaseService.holdLock(name, lockName, lockTimeSecond);
        return success;
    }

}
