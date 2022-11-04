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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IPUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.lease.model.LeaseInfo;
import org.apache.rocketmq.streams.lease.service.ILeaseGetCallback;
import org.apache.rocketmq.streams.lease.service.ILeaseService;
import org.apache.rocketmq.streams.lease.service.ILeaseStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BasedLesaseImpl implements ILeaseService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BasedLesaseImpl.class);

    private static final String CONSISTENT_HASH_PREFIX = "consistent_hash_";
    private static final AtomicBoolean syncStart = new AtomicBoolean(false);
    private static final int synTime = 120;  // 5分钟的一致性hash同步时间太久了，改为2分钟
    protected ScheduledExecutorService taskExecutor = null;
    protected int leaseTerm = 300 * 2;                                  // 租约时间

    // protected transient JDBCDriver jdbcDataSource = null;
    protected ILeaseStorage leaseStorage;
    protected volatile Map<String, Date> leaseName2Date = new ConcurrentHashMap<>();    // 每个lease name对应的租约到期时间

    public BasedLesaseImpl() {

        taskExecutor = new ScheduledThreadPoolExecutor(10);
    }

    /**
     * lease_name: consistent_hash_ip, lease_user_ip: ip,定时刷新lease_info表，检查一致性hash环的节点情况
     *
     * @param name
     * @return
     */
    @Override
    public boolean hasLease(String name) {
        // 内存中没有租约信息则表示 没有租约
        Date leaseEndTime = leaseName2Date.get(name);
        if (leaseEndTime == null) {
            // LOG.info("内存中根据 " + name + "没有查询到租约信息，表示没有租约");
            return false;
        }
        // LOG.info("查询是否有租约 name:" + name + " ,当前时间：" + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
        // + " 租约到期时间 " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(leaseEndTime));
        // 有租约时间，并且租约时间大于当前时间，表示有租约信息
        if (new Date().before(leaseEndTime)) {
            return true;
        }

        return false;
    }

    private final Map<String, AtomicBoolean> startLeaseMap = new HashMap<>();

    @Override
    public void startLeaseTask(final String name) {
        startLeaseTask(name, this.leaseTerm, null);
    }

    @Override
    public void startLeaseTask(final String name, ILeaseGetCallback callback) {
        startLeaseTask(name, this.leaseTerm, callback);
    }

    @Override
    public void startLeaseTask(final String name, int leaseTerm, ILeaseGetCallback callback) {
        ApplyTask applyTask = new ApplyTask(leaseTerm, name, callback);
        startLeaseTask(name, applyTask, leaseTerm / 2, true);
    }

    /**
     * 启动定时器，定时执行任务，确保任务可重入
     *
     * @param name
     * @param runnable     具体任务
     * @param scheduleTime 调度时间
     * @param startNow     是否立刻启动一次
     */
    protected void startLeaseTask(final String name, Runnable runnable, int scheduleTime, boolean startNow) {
        AtomicBoolean isStartLease = startLeaseMap.get(name);//多次调用，只启动一次定时任务
        if (isStartLease == null) {
            synchronized (this) {
                isStartLease = startLeaseMap.get(name);
                if (isStartLease == null) {
                    isStartLease = new AtomicBoolean(false);
                    startLeaseMap.put(name, isStartLease);
                }
            }
        }
        if (isStartLease.compareAndSet(false, true)) {
            if (startNow) {
                runnable.run();
            }
            taskExecutor.scheduleWithFixedDelay(runnable, 0, scheduleTime, TimeUnit.SECONDS);
        }
    }

    /**
     * 续约任务
     */
    protected class ApplyTask implements Runnable {

        protected String name;
        protected int leaseTerm;
        protected ILeaseGetCallback callback;

        public ApplyTask(int leaseTerm, String name) {
            this(leaseTerm, name, null);
        }

        public ApplyTask(int leaseTerm, String name, ILeaseGetCallback callback) {
            this.name = name;
            this.leaseTerm = leaseTerm;
            this.callback = callback;
        }

        @Override
        public void run() {
            try {
                // LOG.info("LeaseServiceImpl name: " + name + "开始获取租约...");
                AtomicBoolean newApplyLease = new AtomicBoolean(false);
                Date leaseDate = applyLeaseTask(leaseTerm, name, newApplyLease);
                if (leaseDate != null) {
                    leaseName2Date.put(name, leaseDate);
                    LOGGER.info("LeaseServiceImpl, name: " + name + " " + getSelfUser() + " 获取租约成功, 租约到期时间为 "
                        + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(leaseDate));
                } else {
                    // fix.2020.08.13 这时name对应的租约可能还在有效期内,或者本机还持有租约，需要remove
                    //  leaseName2Date.remove(name);
                    LOGGER.info("LeaseServiceImpl name: " + name + " " + getSelfUser() + " 获取租约失败 ");
                }
                if (newApplyLease.get() && callback != null) {
                    callback.callback(leaseDate);
                }
            } catch (Exception e) {
                LOGGER.error(" LeaseServiceImpl name: " + name + "  " + getSelfUser() + " 获取租约出现异常 ", e);
            }

        }
    }

    /**
     * 申请租约，如果当期租约有效，直接更新一个租约周期，如果当前租约无效，先查询是否有有效的租约，如果有申请失败，否则直接申请租约
     */
    protected Date applyLeaseTask(int leaseTerm, String name, AtomicBoolean newApplyLease) {

        // 计算下一次租约时间 = 当前时间 + 租约时长
        Date nextLeaseDate = DateUtil.addSecond(new Date(), leaseTerm);

        // 1 如果已经有租约，则更新租约时间(内存和数据库)即可
        if (hasLease(name)) {
            // LOG.info("用户已有租约，更新数据库和内存中的租约信息");
            // 更新数据库
            LeaseInfo leaseInfo = queryValidateLease(name);
            if (leaseInfo == null) {
                LOGGER.error("LeaseServiceImpl applyLeaseTask leaseInfo is null");
                return null;
            }
            // fix.2020.08.13,与本机ip相等且满足一致性hash分配策略，才续约，其他情况为null
            String leaseUserIp = leaseInfo.getLeaseUserIp();
            if (!leaseUserIp.equals(getSelfUser())) {
                return null;
            }
            leaseInfo.setLeaseEndDate(nextLeaseDate);
            updateLeaseInfo(leaseInfo);
            return nextLeaseDate;
        }

        // 2 没有租约情况 判断是否可以获取租约，只要租约没有被其他人获取，则说明有有效租约
        boolean success = canGetLease(name);
        if (!success) { // 表示被其他机器获取到了有效的租约
            // LOG.info("其他机器获取到了有效的租约");
            return null;
        }

        // 3 没有租约而且可以获取租约的情况，则尝试使用数据库原子更新的方式获取租约，保证只有一台机器成功获取租约，而且可以运行
        boolean flag = tryGetLease(name, nextLeaseDate);
        if (flag) { // 获取租约成功
            newApplyLease.set(true);
            return nextLeaseDate;
        }
        return null;

    }

    /**
     * 查询数据库，自己是否在租期内或没有被其他人租用
     *
     * @return
     */
    protected boolean canGetLease(String name) {
        LeaseInfo leaseInfo = queryValidateLease(name);
        if (leaseInfo == null) {
            return true;
        }
        // fix.2020.08.13,租约ip为本机ip，且与一致性hash分配ip一致，才是有效租约
        String leaseUserIp = leaseInfo.getLeaseUserIp();
        if (leaseUserIp.equals(getSelfUser())) {
            return true;
        }
        return false;
    }

    /**
     * 更新数据库，占用租期并更新租期时间
     *
     * @param time
     */
    protected boolean tryGetLease(String name, Date time) {
        // LOG.info("尝试获取租约 lease name is : " + name + " 下次到期时间: "
        // + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(time));
        LeaseInfo validateLeaseInfo = queryValidateLease(name);

        if (validateLeaseInfo == null) {// 这里有两种情况 1 数据库里面没有租约信息 2 数据库里面有租约信息但是已经过期
            Integer count = countLeaseInfo(name);
            if (count == null || count == 0) {// 表示现在数据库里面没有任何租约信息，插入租约成功则表示获取成功，失败表示在这一时刻其他机器获取了租约
                // LOG.info("数据库中暂时没有租约信息，尝试原子插入租约:" + name);
                // fix.2020.08.13,经过一致性hash计算，该名字的任务不应该在本机执行，直接返回，无需插入。只有分配到hash执行权限的机器才可以插入并获取租约
                if (!getSelfUser().equals(getConsistentHashHost(name))) {
                    return false;
                }
                validateLeaseInfo = new LeaseInfo();
                validateLeaseInfo.setLeaseName(name);
                validateLeaseInfo.setLeaseUserIp(getSelfUser());
                validateLeaseInfo.setLeaseEndDate(time);
                validateLeaseInfo.setStatus(1);
                validateLeaseInfo.setVersion(1);
                if (insert(validateLeaseInfo)) {
                    LOGGER.info("数据库中暂时没有租约信息，原子插入成功，获取租约成功:" + name);
                    return true;
                } else {
                    LOGGER.info("数据库中暂时没有租约信息，原子插入失败，已经被其他机器获取租约:" + name);
                    return false;
                }
            } else { // 表示数据库里面有一条但是无效，这里需要两台机器按照version进行原子更新，更新成功的获取租约
                // LOG.info("数据库中有一条无效的租约信息，尝试根据版本号去原子更新租约信息:" + name);
                LeaseInfo inValidateLeaseInfo = queryInValidateLease(name);
                if (inValidateLeaseInfo == null) {// 说明这个时候另外一台机器获取成功了
                    LOGGER.info("另外一台机器获取成功了租约:" + name);
                    return false;
                }
                // fix.2020.08.13,机器重启之后，该名字的任务已经不分配在此机器上执行，直接返回，无需更新数据库
                if (!getSelfUser().equals(getConsistentHashHost(name))) {
                    return false;
                }
                inValidateLeaseInfo.setLeaseName(name);
                inValidateLeaseInfo.setLeaseUserIp(getSelfUser());
                inValidateLeaseInfo.setLeaseEndDate(time);
                inValidateLeaseInfo.setStatus(1);
                boolean success = updateDBLeaseInfo(inValidateLeaseInfo);
                if (success) {
                    LOGGER.info("LeaseServiceImpl 原子更新租约成功，当前机器获取到了租约信息:" + name);
                } else {
                    LOGGER.info("LeaseServiceImpl 原子更新租约失败，租约被其他机器获取:" + name);
                }
                return success;
            }

        } else { // 判断是否是自己获取了租约，如果是自己获取了租约则更新时间（内存和数据库），
            // 这里是为了解决机器重启的情况，机器重启，内存中没有租约信息，但是实际上该用户是有租约权限的
            // fix.2020.08.13,租约的ip与本机ip相等，且满足一致性hash策略，才会被本机执行
            String leaseUserIp = validateLeaseInfo.getLeaseUserIp();
            if (leaseUserIp.equals(getSelfUser())) {
                // 如果当期用户有租约信息，则更新数据库
                validateLeaseInfo.setLeaseEndDate(time);
                boolean hasUpdate = updateLeaseInfo(validateLeaseInfo);
                if (hasUpdate) {
                    LOGGER.info(
                        "LeaseServiceImpl机器重启情况，当前用户有租约信息，并且更新数据库成功，租约信息为 name :" + validateLeaseInfo.getLeaseName()
                            + " ip : " + validateLeaseInfo.getLeaseUserIp() + " 到期时间 : " + new SimpleDateFormat(
                            "yyyy-MM-dd HH:mm:ss").format(validateLeaseInfo.getLeaseEndDate()));
                    return true;
                } else {
                    LOGGER.info("LeaseServiceImpl 机器重启情况，当前用户有租约信息，并且更新数据库失败，表示失去租约:" + name);
                    return false;
                }
            }
            // LOG.info("LeaseServiceImpl 租约被其他机器获取，租约信息为 name :" + validateLeaseInfo.getLeaseName() + " ip : "
            // + validateLeaseInfo.getLeaseUserIp() + " 到期时间 : "
            // + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(validateLeaseInfo.getLeaseEndDate()));
            return false;
        }

    }

    protected LeaseInfo queryValidateLease(String name) {
        //String sql = "SELECT * FROM lease_info WHERE lease_name ='" + name + "' and status=1 and lease_end_time>now()";
        //// LOG.info("LeaseServiceImpl query validate lease sql:" + sql);
        //return queryLease(name, sql);
        return leaseStorage.queryValidateLease(name);
    }

    protected List<LeaseInfo> queryValidateLeaseByNamePrefix(String namePrefix) {
        return leaseStorage.queryValidateLeaseByNamePrefix(namePrefix);
    }

    /**
     * 如果发生唯一索引冲突返回失败
     *
     * @param leaseInfo
     * @return
     */
    private boolean insert(LeaseInfo leaseInfo) {
        try {
            addLeaseInfo(leaseInfo);
            return true;
        } catch (Exception e) {
            LOGGER.error("LeaseServiceImpl insert error", e);
            return false;
        }
    }

    /**
     * 更新时需要加version＝当前version，如果更新数据条数为0，返回false
     *
     * @param leaseInfo
     * @return
     */
    protected boolean updateDBLeaseInfo(LeaseInfo leaseInfo) {
        return updateLeaseInfo(leaseInfo);
    }

    protected boolean updateLeaseInfo(LeaseInfo leaseInfo) {

        return leaseStorage.updateLeaseInfo(leaseInfo);
    }

    protected Integer countLeaseInfo(String name) {

        return leaseStorage.countLeaseInfo(name);
    }

    protected LeaseInfo queryInValidateLease(String name) {

        return leaseStorage.queryInValidateLease(name);
    }

    protected void addLeaseInfo(LeaseInfo leaseInfo) {

        leaseStorage.addLeaseInfo(leaseInfo);

    }

    /**
     * 本地ip地址作为自己的唯一标识
     *
     * @return
     */
    public static String getLocalName() {
        return IPUtil.getLocalIdentification() + ":" + Optional.ofNullable(RuntimeUtil.getPid()).orElse("UNKNOWN");
    }

    /**
     * 本地ip地址作为自己的唯一标识
     *
     * @return
     */
    public String getSelfUser() {
        return getLocalName();
    }

    private String getConsistentHashHost(String name) {
        //if (StringUtil.isEmpty(leaseConsistentHashSuffix)) {
        //    return getSelfUser();
        //}
        //return consistentHashInstance.getCandidateNode(name);
        return getSelfUser();
    }

    public void setLeaseStorage(ILeaseStorage leaseStorage) {
        this.leaseStorage = leaseStorage;
    }
}
