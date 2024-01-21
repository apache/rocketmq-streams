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
package org.apache.rocketmq.streams.lease.service;

import java.util.List;
import org.apache.rocketmq.streams.lease.model.LeaseInfo;

/**
 * 通过db实现租约和锁，可以更轻量级，减少其他中间件的依赖 使用主备场景，只有一个实例运行，当当前实例挂掉，在一定时间内，会被其他实例接手 也可以用于全局锁
 */
public interface ILeaseService {

    /**
     * 默认锁定时间
     */
    static final int DEFAULT_LOCK_TIME = 60 * 5;

    /**
     * 检查某用户当前时间是否具有租约。这个方法是纯内存操作，无性能开销
     *
     * @return true，租约有效；false，租约无效
     */
    boolean hasLease(String name);

    /**
     * 申请租约，会启动一个线程，不停申请租约，直到申请成功。 申请成功后，每 租期/2 续约。 如果目前被其他租户获取租约，只有在对方租约失效，后才允许新的租户获取租约
     *
     * @param name 租约名称，无特殊要求，相同名称会竞争租约
     */
    void startLeaseTask(String name);

    /**
     * 申请租约，会启动一个线程，不停申请租约，直到申请成功。 申请成功后，每 租期/2 续约。 如果目前被其他租户获取租约，只有在对方租约失效，后才允许新的租户获取租约
     *
     * @param name     租约名称，无特殊要求，相同名称会竞争租约
     * @param callback 当第一获取租约时，回调此函数
     */
    void startLeaseTask(final String name, ILeaseGetCallback callback);

    /**
     * 申请租约，会启动一个线程，不停申请租约，直到申请成功。 申请成功后，每 租期/2 续约。 如果目前被其他租户获取租约，只有在对方租约失效，后才允许新的租户获取租约
     *
     * @param name            租约名称，无特殊要求，相同名称会竞争租约
     * @param leaseTermSecond 租期，在租期内可以做业务处理，单位是秒
     * @param callback        当第一获取租约时，回调此函数
     */
    void startLeaseTask(final String name, int leaseTermSecond, ILeaseGetCallback callback);

    /**
     * 释放租约，并且停止底层的调度线程
     *
     * @param name 租约名称
     */
    void stopLeaseTask(final String name);

    /**
     * 申请锁,无论成功与否，立刻返回。如果不释放，最大锁定时间是5分钟
     *
     * @param name       业务名称
     * @param lockerName 锁名称
     * @return 是否加锁成功
     */
    boolean lock(String name, String lockerName);

    /**
     * 申请锁,无论成功与否，立刻返回。默认锁定时间是5分钟
     *
     * @param name           业务名称
     * @param lockerName     锁名称
     * @param lockTimeSecond 如果不释放，锁定的最大时间，单位是秒
     * @return 是否加锁成功
     */
    boolean lock(String name, String lockerName, int lockTimeSecond);

    /**
     * 申请锁，如果没有则等待，等待时间可以指定，如果是－1 则无限等待。如果不释放，最大锁定时间是5分钟
     *
     * @param name       业务名称
     * @param lockerName 锁名称
     * @param waitTime   没获取锁时，最大等待多长时间，如果是－1 则无限等待
     * @return 是否加锁成功
     */
    boolean tryLocker(String name, String lockerName, long waitTime);

    /**
     * 申请锁，如果没有则等待，等待时间可以指定，如果是－1 则无限等待。如果不释放，最大锁定时间是lockTimeSecond
     *
     * @param name           业务名称
     * @param lockerName     锁名称
     * @param waitTime       没获取锁时，最大等待多长时间，如果是－1 则无限等待
     * @param lockTimeSecond 如果不释放，锁定的最大时间，单位是秒
     * @return 是否加锁成功
     */
    boolean tryLocker(String name, String lockerName, long waitTime, int lockTimeSecond);

    /**
     * 释放锁
     *
     * @param name       业务名称
     * @param lockerName 锁名称
     * @return 是否释放成功
     */
    boolean unlock(String name, String lockerName);

    /**
     * 对于已经获取锁的，可以通过这个方法，一直持有锁。 和租约的区别是，当释放锁后，无其他实例抢占。无法实现主备模式
     *
     * @param name           业务名称
     * @param lockerName     锁名称
     * @param lockTimeSecond 租期，这个方法会自动续约，如果不主动释放，会一直持有锁
     * @return 是否成功获取锁
     */
    boolean holdLock(String name, String lockerName, int lockTimeSecond);

    /**
     * 是否持有锁，不会申请锁。如果以前申请过，且未过期，返回true，否则返回false
     *
     * @param name       业务名称
     * @param lockerName 锁名称
     * @return 是否持有锁
     */
    boolean hasHoldLock(String name, String lockerName);

    /**
     * 获取该业务下的所有锁
     *
     * @param name             业务名称
     * @param lockerNamePrefix 锁名前缀
     * @return 所有的锁
     */
    List<LeaseInfo> queryLockedInstanceByNamePrefix(String name, String lockerNamePrefix);

}
