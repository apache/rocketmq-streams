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
package org.apache.rocketmq.streams.dim.builder;

import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.dim.model.DBDim;

public class DimBuilder {

    protected Long pollingTimeSeconds = 60 * 10L;                    // 同步数据的事件间隔
    private String url;
    private String password;
    private String userName;
    private String jdbcDriver = ConfigurationKey.DEFAULT_JDBC_DRIVER;

    public DimBuilder(String url, String userName, String password) {
        this.url = url;
        this.password = password;
        this.userName = userName;
    }

    public DBDim createDim(String namespace, String name, String sqlOrTableName) {
        DBDim nameList = new DBDim();
        nameList.setNameSpace(namespace);
        if (StringUtil.isNotEmpty(name)) {
            nameList.setName(name);
        }
        String sql = sqlOrTableName;
        if (sqlOrTableName.split(" ").length == 1) {
            sql = "select * from " + sqlOrTableName + " limit 500000";
        }
        nameList.setSql(sql);
        nameList.setJdbcDriver(jdbcDriver);
        nameList.setPollingTimeSeconds(pollingTimeSeconds);
        nameList.setUrl(url);
        nameList.setUserName(userName);
        nameList.setPassword(password);
        return nameList;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public Long getPollingTimeSeconds() {
        return pollingTimeSeconds;
    }

    public void setPollingTimeSeconds(Long pollingTimeSeconds) {
        this.pollingTimeSeconds = pollingTimeSeconds;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }
}
