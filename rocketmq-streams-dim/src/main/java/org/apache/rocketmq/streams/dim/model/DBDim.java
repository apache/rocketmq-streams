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
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.db.driver.batchloader.BatchRowLoader;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBDim extends AbstractDim {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBDim.class);

    private String jdbcDriver = "com.mysql.jdbc.Driver";

    @ENVDependence
    private String url;

    @ENVDependence
    private String userName;

    @ENVDependence
    private String password;

    private String sql;//sql 会被定时执行

    protected String idFieldName;

    private static transient AtomicInteger nameCreator = new AtomicInteger(0);

    /**
     * 是否支持批量查找
     */
    protected transient Boolean supportBatch = false;

    public DBDim() {
        this.setType(TYPE);
    }

    @Override
    protected void loadData2Memory(AbstractMemoryTable tableCompress) {
//        if (StringUtil.isNotEmpty(idFieldName)) {
//            BatchRowLoader batchRowLoader = new BatchRowLoader(idFieldName, sql, new IRowOperator() {
//                @Override
//                public synchronized void doProcess(Map<String, Object> row) {
//                    tableCompress.addRow(row);
//                }
//            }, DriverBuilder.DEFALUT_JDBC_DRIVER, url, userName, password);
//            batchRowLoader.startLoadData();
//            return;
//        }
        List<Map<String, Object>> rows = executeQuery();

        for (Map<String, Object> row : rows) {
            tableCompress.addRow(row);
        }
    }

    protected List<Map<String, Object>> executeQuery() {
        JDBCDriver resource = createResource();
        try {
            List<Map<String, Object>> result = resource.queryForList(sql);
            ;
            LOGGER.info("load configurable's count is " + result.size());
            return result;
        } finally {
            if (resource != null) {
                resource.destroy();
            }
        }

    }

    protected JDBCDriver createResource() {
        return DriverBuilder.createDriver(jdbcDriver, url, userName, password);
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
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

    public String getJdbcDriver() {
        return jdbcDriver;
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

    public String getIdFieldName() {
        return idFieldName;
    }

    public void setIdFieldName(String idFieldName) {
        this.idFieldName = idFieldName;
    }

    public void setSupportBatch(Boolean supportBatch) {
        this.supportBatch = supportBatch;
    }

}
