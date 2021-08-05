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
package org.apache.rocketmq.streams.lease.service.storages;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.lease.model.LeaseInfo;
import org.apache.rocketmq.streams.lease.service.ILeaseStorage;

public class DBLeaseStorage implements ILeaseStorage {
    private static final Log LOG = LogFactory.getLog(DBLeaseStorage.class);
    protected JDBCDriver jdbcDataSource;
    private String url;
    protected String userName;
    protected String password;
    protected String jdbc;

    public DBLeaseStorage(String jdbc, String url, String userName, String password) {
        this.jdbc = jdbc;
        this.url = url;
        this.userName = userName;
        this.password = password;
        jdbcDataSource = DriverBuilder.createDriver(jdbc, url, userName, password);
    }

    @Override
    public boolean updateLeaseInfo(LeaseInfo leaseInfo) {
        String sql = "UPDATE lease_info SET version=version+1,status=#{status},gmt_modified=now()";
        String whereSQL = " WHERE id=#{id} and version=#{version}";

        if (StringUtil.isNotEmpty(leaseInfo.getLeaseName())) {
            sql += ",lease_name=#{leaseName}";
        }
        if (StringUtil.isNotEmpty(leaseInfo.getLeaseUserIp())) {
            sql += ",lease_user_ip=#{leaseUserIp}";
        }
        if (leaseInfo.getLeaseEndDate() != null) {
            sql += ",lease_end_time=#{leaseEndDate}";
        }
        sql += whereSQL;
        sql = SQLUtil.parseIbatisSQL(leaseInfo, sql);
        try {
            int count = getOrCreateJDBCDataSource().update(sql);
            boolean success = count > 0;
            if (success) {
                synchronized (this) {
                    leaseInfo.setVersion(leaseInfo.getVersion() + 1);
                }
            } else {
                System.out.println(count);
            }
            return success;
        } catch (Exception e) {
            LOG.error("LeaseServiceImpl updateLeaseInfo excuteUpdate error", e);
            throw new RuntimeException("execute sql error " + sql, e);
        }
    }

    @Override
    public Integer countLeaseInfo(String leaseName) {
        String sql = "SELECT count(*) as c FROM lease_info  WHERE lease_name = '" + leaseName + "' and status = 1";
        try {

            List<Map<String, Object>> rows = getOrCreateJDBCDataSource().queryForList(sql);
            if (rows == null || rows.size() == 0) {
                return null;
            }
            Long value = (Long)rows.get(0).get("c");
            return value.intValue();
        } catch (Exception e) {
            throw new RuntimeException("execute sql error " + sql, e);
        }
    }

    @Override
    public LeaseInfo queryInValidateLease(String leaseName) {
        String sql = "SELECT * FROM lease_info WHERE lease_name ='" + leaseName + "' and status=1 and lease_end_time<'" + DateUtil.getCurrentTimeString() + "'";
        LOG.info("LeaseServiceImpl queryInValidateLease builder:" + sql);
        return queryLease(leaseName, sql);
    }

    @Override
    public LeaseInfo queryValidateLease(String leaseName) {
        String sql = "SELECT * FROM lease_info WHERE lease_name ='" + leaseName + "' and status=1 and lease_end_time>now()";
        return queryLease(leaseName, sql);
    }

    @Override
    public List<LeaseInfo> queryValidateLeaseByNamePrefix(String namePrefix) {
        String sql = "SELECT * FROM lease_info WHERE lease_name like '" + namePrefix + "%' and status=1 and lease_end_time>now()";
        try {
            List<LeaseInfo> leaseInfos = new ArrayList<>();
            List<Map<String, Object>> rows = getOrCreateJDBCDataSource().queryForList(sql);
            if (rows == null || rows.size() == 0) {
                return null;
            }
            for (Map<String, Object> row : rows) {
                LeaseInfo leaseInfo = convert(row);
                leaseInfos.add(leaseInfo);
            }

            return leaseInfos;
        } catch (Exception e) {
            throw new RuntimeException("execute sql error " + sql, e);
        }
    }

    @Override
    public void addLeaseInfo(LeaseInfo leaseInfo) {
        String sql =
            " REPLACE INTO lease_info(lease_name,lease_user_ip,lease_end_time,status,version,gmt_create,gmt_modified)"
                + " VALUES (#{leaseName},#{leaseUserIp},#{leaseEndDate},#{status},#{version},now(),now())";
        sql = SQLUtil.parseIbatisSQL(leaseInfo, sql);
        try {

            getOrCreateJDBCDataSource().execute(sql);
        } catch (Exception e) {
            LOG.error("LeaseServiceImpl execute sql error,sql:" + sql, e);
            throw new RuntimeException("execute sql error " + sql, e);
        }
    }

    protected JDBCDriver getOrCreateJDBCDataSource() {
        if (this.jdbcDataSource == null || !this.jdbcDataSource.isValidate()) {
            synchronized (this) {
                if (this.jdbcDataSource == null || !this.jdbcDataSource.isValidate()) {
                    this.jdbcDataSource =
                        DriverBuilder.createDriver(this.jdbc, this.url, this.userName, this.password);
                }
            }
        }
        return jdbcDataSource;
    }

    protected LeaseInfo queryLease(String name, String sql) {
        try {
            List<Map<String, Object>> rows = getOrCreateJDBCDataSource().queryForList(sql);
            if (rows == null || rows.size() == 0) {
                return null;
            }
            return convert(rows.get(0));
        } catch (Exception e) {
            throw new RuntimeException("execute sql error " + sql, e);
        }
    }

    protected LeaseInfo convert(Map<String, Object> map) {
        LeaseInfo leaseInfo = new LeaseInfo();
        leaseInfo.setId(getMapLongValue("id", map));
        leaseInfo.setCreateTime(getMapDateValue("gmt_create", map));
        leaseInfo.setLeaseEndDate(getMapDateValue("lease_end_time", map));
        leaseInfo.setLeaseName(getMapValue("lease_name", map, String.class));
        leaseInfo.setLeaseUserIp(getMapValue("lease_user_ip", map, String.class));
        Integer status = getMapValue("status", map, Integer.class);
        if (status != null) {
            leaseInfo.setStatus(status);
        }
        leaseInfo.setUpdateTime(getMapDateValue("gmt_modified", map));
        Long version = getMapLongValue("version", map);
        if (version != null) {
            leaseInfo.setVersion(version);
        }
        return leaseInfo;
    }

    @SuppressWarnings("unchecked")
    private <T> T getMapValue(String fieldName, Map<String, Object> map, Class<T> integerClass) {
        Object value = map.get(fieldName);
        if (value == null) {
            return null;
        }
        return (T)value;
    }

    private Long getMapLongValue(String fieldName, Map<String, Object> map) {
        Object value = map.get(fieldName);
        if (value == null) {
            return null;
        }
        if (value instanceof Long) {
            return (Long)value;
        }
        if (value instanceof BigInteger) {
            return ((BigInteger)value).longValue();
        }
        return null;
    }

    private Date getMapDateValue(String fieldName, Map<String, Object> map) {
        Object value = map.get(fieldName);
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return (Date)value;
        }
        if (value instanceof String) {
            return DateUtil.parseTime(((String)value));
        }
        return null;

    }

}
