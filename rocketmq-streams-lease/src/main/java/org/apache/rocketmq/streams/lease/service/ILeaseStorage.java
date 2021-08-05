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

public interface ILeaseStorage {

    /**
     * 更新lease info，需要是原子操作，存储保障多线程操作的原子性
     *
     * @param leaseInfo 租约表数据
     * @return
     */
    boolean updateLeaseInfo(LeaseInfo leaseInfo);

    /**
     * 统计这个租约名称下，LeaseInfo对象个数
     *
     * @param leaseName 租约名称，无特殊要求，相同名称会竞争租约
     * @return
     */
    Integer countLeaseInfo(String leaseName);

    /**
     * 查询无效的的租约
     *
     * @param leaseName 租约名称，无特殊要求，相同名称会竞争租约
     * @return
     */
    LeaseInfo queryInValidateLease(String leaseName);

    /**
     * 查询有效的的租约
     *
     * @param leaseName 租约名称，无特殊要求，相同名称会竞争租约
     * @return
     */
    LeaseInfo queryValidateLease(String leaseName);

    /**
     * 按前缀查询有效的租约信息
     *
     * @param namePrefix
     * @return
     */
    List<LeaseInfo> queryValidateLeaseByNamePrefix(String namePrefix);

    /**
     * 增加租约
     *
     * @param leaseInfo 租约名称，无特殊要求，相同名称会竞争租约
     */
    void addLeaseInfo(LeaseInfo leaseInfo);

}
