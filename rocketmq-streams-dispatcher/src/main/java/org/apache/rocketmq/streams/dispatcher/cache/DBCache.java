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
package org.apache.rocketmq.streams.dispatcher.cache;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.dispatcher.ICache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBCache implements ICache {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBCache.class);

    private final String cacheTable = "dipper_dispatcher_kv_config";
    private final String url;
    private final String userName;
    private final String password;

    public DBCache() {
        try {
            String driver = SystemContext.getProperty(ConfigurationKey.JDBC_DRIVER);
            if (StringUtil.isEmpty(driver)) {
                driver = ConfigurationKey.DEFAULT_JDBC_DRIVER;
            }
            this.url = SystemContext.getProperty(ConfigurationKey.JDBC_URL);
            this.userName = SystemContext.getProperty(ConfigurationKey.JDBC_USERNAME);
            this.password = SystemContext.getProperty(ConfigurationKey.JDBC_PASSWORD);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public DBCache(Properties properties) {
        try {
            String driver = properties.getProperty(ConfigurationKey.JDBC_DRIVER);
            this.url = properties.getProperty(ConfigurationKey.JDBC_URL);
            this.userName = properties.getProperty(ConfigurationKey.JDBC_USERNAME);
            this.password = properties.getProperty(ConfigurationKey.JDBC_PASSWORD);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putKeyConfig(String namespace, String key, String value) {
        try {
            String sql = "insert into " + cacheTable + " (namespace, config_key, config_value) values (?, ?, ?) on duplicate key update namespace=?, config_key=?, config_value=?";
            execute(sql, new String[] {namespace, key, value, namespace, key, value});
        } catch (Exception e) {
            LOGGER.error("Put_Key_Config_Error", e);
        }
    }

    @Override
    public String getKeyConfig(String namespace, String key) {
        String configValue = null;
        try {
            String sql = "select config_value from " + cacheTable + " where namespace=? and config_key=?";
            List<Map<String, String>> result = executeQuery(sql, new String[] {namespace, key}, new String[] {"config_value"});
            if (!result.isEmpty()) {
                configValue = result.get(0).get("config_value");
            }
        } catch (Exception e) {
            LOGGER.error("Get_Key_Config_Error", e);
        }
        return configValue;
    }

    @Override
    public void deleteKeyConfig(String namespace, String key) {
        try {
            String sql = "delete from " + cacheTable + " where namespace=? and config_key=?";
            execute(sql, new String[] {namespace, key});
        } catch (Exception e) {
            LOGGER.error("Delete_Key_Config_Error", e);
        }
    }

    @Override
    public HashMap<String, String> getKVListByNameSpace(String namespace) {
        HashMap<String, String> kv = Maps.newHashMap();
        try {
            String sql = "select config_key, config_value from " + cacheTable + " where namespace=?";
            List<Map<String, String>> result = executeQuery(sql, new String[] {namespace}, new String[] {"config_key", "config_value"});
            for (Map<String, String> data : result) {
                kv.put(data.get("config_key"), data.get("config_value"));
            }
        } catch (Exception e) {
            LOGGER.error("Get_KVList_Config_Error", e);
        }
        return kv;
    }

    private void execute(String sql, String[] params) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            connection = DriverManager.getConnection(this.url, this.userName, this.password);
            preparedStatement = connection.prepareStatement(sql);
            for (int i = 1; i <= params.length; i++) {
                preparedStatement.setString(i, params[i - 1]);
            }
            preparedStatement.execute();
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
    }

    private List<Map<String, String>> executeQuery(String sql, String[] params, String[] columns) throws Exception {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        List<Map<String, String>> result = Lists.newArrayList();
        try {
            connection = DriverManager.getConnection(this.url, this.userName, this.password);
            preparedStatement = connection.prepareStatement(sql);
            for (int i = 1; i <= params.length; i++) {
                preparedStatement.setString(i, params[i - 1]);
            }
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                Map<String, String> data = Maps.newHashMap();
                for (String column : columns) {
                    data.put(column, resultSet.getString(column));
                }
                result.add(data);
            }
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
        }
        return result;
    }

}
