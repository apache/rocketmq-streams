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
package org.apache.rocketmq.streams.lease.service.impl;

import java.util.Date;
import java.util.List;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.lease.model.LeaseInfo;
import org.apache.rocketmq.streams.lease.service.ILeaseGetCallback;
import org.apache.rocketmq.streams.lease.service.ILeaseService;

/**
 * 在内存和文件模式下使用，所有的申请都会返回true，主要用来做业务测试
 */
public class MockLeaseImpl implements ILeaseService {
    @Override
    public boolean hasLease(String name) {
        return true;
    }

    @Override
    public void startLeaseTask(String name) {

    }

    @Override
    public void startLeaseTask(String name, ILeaseGetCallback callback) {
        callback.callback(DateUtil.addMinute(new Date(), 1));
    }

    @Override
    public void startLeaseTask(String name, int leaseTerm, ILeaseGetCallback callback) {

    }

    @Override
    public void stopLeaseTask(String name) {

    }

    @Override
    public boolean lock(String name, String lockerName) {
        return true;
    }

    @Override
    public boolean lock(String name, String lockerName, int lockTimeSecond) {
        return true;
    }

    @Override
    public boolean tryLocker(String name, String lockerName, long waitTime) {
        return true;
    }

    @Override
    public boolean tryLocker(String name, String lockerName, long waitTime, int lockTimeSecond) {
        return true;
    }

    @Override
    public boolean unlock(String name, String lockerName) {
        return true;
    }

    @Override
    public boolean holdLock(String name, String lockerName, int lockTimeSecond) {
        return true;
    }

    @Override
    public boolean hasHoldLock(String name, String lockerName) {
        return true;
    }

    @Override
    public List<LeaseInfo> queryLockedInstanceByNamePrefix(String name, String lockerNamePrefix) {
        return null;
    }

}
