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
package org.apache.rocketmq.streams.lease.model;

/**
 * 租约对象，需要创建租约表。
 */

import java.io.Serializable;
import java.util.Date;

public class LeaseInfo implements Serializable {
    private static final long serialVersionUID = 665608838255753618L;
    private Long id;
    private Date createTime;
    private Date updateTime;
    private String leaseName;//租约名称，多个进程共享一个租约，只要名称相同即可
    private String leaseUserIp;//区分不同的租约实体，以前默认用ip，但一个机器多个进程的情况下，用ip会区分不开，后续会加上进程号
    private Date leaseEndDate;//租约到期时间
    private int status;//租约的有效状态
    private long version;//版本，通过版本保证更新原子性

    public LeaseInfo() {
    }

    /**
     * 建表语句
     *
     * @return
     */
    public static String createTableSQL() {
        return "CREATE TABLE  IF NOT EXISTS  `lease_info` (\n"
            + "  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',\n"
            + "  `gmt_create` datetime NOT NULL COMMENT '创建时间',\n"
            + "  `gmt_modified` datetime NOT NULL COMMENT '修改时间',\n"
            + "  `lease_name` varchar(255) NOT NULL COMMENT '租约名称',\n"
            + "  `lease_user_ip` varchar(255) NOT NULL COMMENT '租者IP',\n"
            + "  `lease_end_time` varchar(255) NOT NULL COMMENT '租约到期时间',\n"
            + "  `status` int(11) NOT NULL DEFAULT '1' COMMENT '状态',\n"
            + "  `version` bigint(20) NOT NULL COMMENT '版本',\n"
            + "  `candidate_lease_ip` varchar(255) DEFAULT NULL COMMENT '候选租约ip',\n"
            + "  PRIMARY KEY (`id`),\n"
            + "  UNIQUE KEY `uk_name` (`lease_name`)\n"
            + ") ENGINE=InnoDB AUTO_INCREMENT=8150 DEFAULT CHARSET=utf8 COMMENT='租约信息'\n"
            + ";";
    }

    public Long getId() {
        return this.id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Date getCreateTime() {
        return this.createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getUpdateTime() {
        return this.updateTime;
    }

    public void setUpdateTime(Date updateTime) {
        this.updateTime = updateTime;
    }

    public String getLeaseUserIp() {
        return this.leaseUserIp;
    }

    public void setLeaseUserIp(String leaseUserIp) {
        this.leaseUserIp = leaseUserIp;
    }

    public Date getLeaseEndDate() {
        return this.leaseEndDate;
    }

    public void setLeaseEndDate(Date leaseEndDate) {
        this.leaseEndDate = leaseEndDate;
    }

    public int getStatus() {
        return this.status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getLeaseName() {
        return this.leaseName;
    }

    public void setLeaseName(String leaseName) {
        this.leaseName = leaseName;
    }

    public long getVersion() {
        return this.version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

}

