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
package org.apache.rocketmq.streams.dim.model;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.CompressTable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.utils.IPUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;

public class DBDim extends AbstractDim {

    private static final Log LOG = LogFactory.getLog(DBDim.class);

    private String jdbcdriver = "com.mysql.jdbc.Driver";

    @ENVDependence
    private String url;

    @ENVDependence
    private String userName;

    @ENVDependence
    private String password;

    private String sql;//sql 会被定时执行

    private static transient AtomicInteger nameCreator = new AtomicInteger(0);

    /**
     * 是否支持批量查找
     */
    protected transient Boolean supportBatch = false;

    public DBDim() {
        this.setConfigureName(MapKeyUtil.createKey(IPUtil.getLocalIdentification(), System.currentTimeMillis() + "",
            nameCreator.incrementAndGet() + ""));
        this.setType(TYPE);
    }

    @Override
    protected CompressTable loadData() {
        List<Map<String, Object>> rows = executeQuery();
        CompressTable tableCompress = new CompressTable();
        for (Map<String, Object> row : rows) {
            tableCompress.addRow(row);
        }
        return tableCompress;
    }

    protected List<Map<String, Object>> executeQuery() {
        JDBCDriver resource = createResouce();
        try {
            List<Map<String, Object>> result = resource.queryForList(sql);
            ;
            LOG.info("load configurable's count is " + result.size());
            return result;
        } finally {
            if (resource != null) {
                resource.destroy();
            }
        }

    }

    protected JDBCDriver createResouce() {
        return DriverBuilder.createDriver(jdbcdriver, url, userName, password);
    }

    public void setJdbcdriver(String jdbcdriver) {
        this.jdbcdriver = jdbcdriver;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getJdbcdriver() {
        return jdbcdriver;
    }

    public String getUrl() {
        return url;
    }

    public String getUserName() {
        return userName;
    }

    public String getPassword() {
        return password;
    }

    public String getSql() {
        return sql;
    }

    public Boolean getSupportBatch() {
        return supportBatch;
    }

    public void setSupportBatch(Boolean supportBatch) {
        this.supportBatch = supportBatch;
    }

}
