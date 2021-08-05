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
package org.apache.rocketmq.streams.db.configuable;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.interfaces.IPropertyEnable;
import org.apache.rocketmq.streams.common.utils.AESUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.configurable.model.Configure;
import org.apache.rocketmq.streams.configurable.service.AbstractConfigurableService;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;

/**
 * Configuable对象存储在db中，是生成环境常用的一种模式 数据库参数可以配置在配置文件中，ConfiguableComponent在启动时，会把参数封装在Properties中，调用DBConfigureService(Properties properties) 构造方法完成实例创建
 */

public class DBConfigureService extends AbstractConfigurableService implements IPropertyEnable {

    private static final Log LOG = LogFactory.getLog(DBConfigureService.class);
    private String jdbcdriver;
    private String url;
    private String userName;
    private String password;
    private String tableName = "dipper_configure";
    @Deprecated
    private boolean isCompatibilityOldRuleEngine = false;//兼容老规则引擎使用，正常场景不需要理会

    public DBConfigureService(String jdbcdriver, String url, String userName, String password) {
        this(jdbcdriver, url, userName, password, null);
    }

    public DBConfigureService(String jdbcdriver, String url, String userName, String password, String tableName) {
        this.url = url;
        this.jdbcdriver = jdbcdriver;
        this.userName = userName;
        this.password = password;
        this.tableName = tableName;
        LOG.info("DBConfigureService resource ,the info is: driver:" + this.jdbcdriver + ",url:" + this.url
            + ",username:" + userName + ",password:" + password);
        regJdbcDriver(jdbcdriver);
    }

    public DBConfigureService() {
    }

    /**
     * @param properties
     */
    public DBConfigureService(Properties properties) {
        super(properties);
        initProperty(properties);
    }

    @Override
    protected GetConfigureResult loadConfigurable(String namespace) {
        GetConfigureResult result = new GetConfigureResult();
        try {
            List<Configure> configures = selectOpening(namespace);
            List<IConfigurable> configurables = convert(configures);
            result.setConfigurables(configurables);
            result.setQuerySuccess(true);// 该字段标示查询是否成功，若不成功则不会更新配置
        } catch (Exception e) {
            result.setQuerySuccess(false);
            LOG.error("load configurable error ", e);
        }
        return result;
    }

    protected List<Configure> selectOpening(String namespace) {
        return queryConfigureByNamespace(namespace);
    }

    protected List<Configure> queryConfigureByNamespace(String... namespaces) {
        return queryConfigureByNamespaceInner(null, namespaces);
    }

    protected List<Configure> queryConfigureByNamespaceInner(String type, String... namespaces) {
        JDBCDriver resource = createResouce();
        try {
            String namespace = "namespace";
            if (isCompatibilityOldRuleEngine && AbstractComponent.JDBC_COMPATIBILITY_RULEENGINE_TABLE_NAME.equals(tableName)) {
                namespace = "name_space";
            }
            String sql = "SELECT * FROM `" + tableName + "` WHERE " + namespace + " in (" + SQLUtil.createInSql(namespaces) + ") and status =1";
            if (StringUtil.isNotEmpty(type)) {
                sql = sql + " and type='" + type + "'";
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("namespace", MapKeyUtil.createKeyBySign(",", namespaces));
            sql = SQLUtil.parseIbatisSQL(jsonObject, sql);
            // String builder = "SELECT * FROM `" + tableName + "` WHERE namespace ='" + namespace + "' and status =1";
            List<Map<String, Object>> result = resource.queryForList(sql);
            if (result == null) {
                return new ArrayList<Configure>();
            }
            // LOG.info("load configurable's count is " + result.size());
            return convert2Configure(result);
        } finally {
            if (resource != null) {
                resource.destroy();
            }
        }
    }

    @Override
    public List<IConfigurable> queryConfiguableByNamespace(String... namespaces) {
        List<Configure> configures = queryConfigureByNamespace(namespaces);
        List<IConfigurable> configurables = convert(configures);
        return configurables;
    }

    public static void main(String[] args) {
        String[] namespaces = new String[] {"rule1", null};
        String sql = "SELECT * FROM `dipper_configure` WHERE namespace in (" + SQLUtil.createInSql(namespaces) + ") and status =1";
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("namespace", MapKeyUtil.createKeyBySign(",", namespaces));
        sql = SQLUtil.parseIbatisSQL(jsonObject, sql);
        System.out.println(sql);
    }

    protected void saveOrUpdate(IConfigurable configure) {
        JDBCDriver jdbcDataSource = createResouce();
        String sql = AbstractConfigurable.createSQL(configure, this.tableName);
        try {
            jdbcDataSource.executeInsert(sql);
        } catch (Exception e) {
            LOG.error("DBConfigureService saveOrUpdate error,sqlnode:" + sql);
            throw new RuntimeException(e);
        } finally {
            if (jdbcDataSource != null) {
                jdbcDataSource.destroy();
            }
        }
    }

    protected List<Configure> convert2Configure(List<Map<String, Object>> rows) {
        List<Configure> configures = new ArrayList<Configure>();
        for (Map<String, Object> row : rows) {
            Configure configure = new Configure();
            Long id = getColumnValue(row, "id");
            configure.setId(id);
            Date create = getColumnValue(row, "gmt_create");
            configure.setGmtCreate(create);
            Date modify = getColumnValue(row, "gmt_modified");
            configure.setGmtModified(modify);
            String namespace = getColumnValue(row, "namespace");
            if (StringUtil.isEmpty(namespace)) {
                namespace = getColumnValue(row, "name_space");
            }
            configure.setNameSpace(namespace);
            String type = getColumnValue(row, "type");
            configure.setType(type);
            String name = getColumnValue(row, "name");
            configure.setName(name);
            String jsonValue = getColumnValue(row, "json_value");
            try {
                jsonValue = AESUtil.aesDecrypt(jsonValue, ConfigureFileKey.SECRECY);
            } catch (Exception e) {
                LOG.error("can't decrypt the value, reason:\t" + e.getCause());
                throw new RuntimeException(e);
            }
            configure.setJsonValue(jsonValue);
            configures.add(configure);
        }
        return configures;
    }

    @SuppressWarnings("unchecked")
    protected <T> T getColumnValue(Map<String, Object> row, String columnName) {
        Object value = row.get(columnName);
        if (value == null) {
            return null;
        }
        if (java.math.BigInteger.class.isInstance(value)) {
            return (T)Long.valueOf(value.toString());
        }
        return (T)value;

    }

    protected JDBCDriver createResouce() {
        JDBCDriver resource = DriverBuilder.createDriver(this.jdbcdriver, this.url, this.userName, this.password);
        return resource;
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

    private void regJdbcDriver(String jdbcdriver) {
        try {
            if (StringUtil.isEmpty(jdbcdriver)) {
                jdbcdriver = AbstractComponent.DEFAULT_JDBC_DRIVER;
            }
            Class.forName(jdbcdriver);
        } catch (ClassNotFoundException e) {
            LOG.error("DBConfigureService regJdbcDriver ClassNotFoundException error", e);
        } catch (Exception e) {
            LOG.error("DBConfigureService regJdbcDriver error", e);
        }
    }

    @Override
    public void initProperty(Properties properties) {
        this.jdbcdriver = properties.getProperty(AbstractComponent.JDBC_DRIVER);
        regJdbcDriver(jdbcdriver);
        this.url = properties.getProperty(AbstractComponent.JDBC_URL);
        this.userName = properties.getProperty(AbstractComponent.JDBC_USERNAME);
        this.password = properties.getProperty(AbstractComponent.JDBC_PASSWORD);
        String tableName = properties.getProperty(AbstractComponent.JDBC_TABLE_NAME);
        String isCompatibilityOldRuleEngine = properties.getProperty(AbstractComponent.JDBC_COMPATIBILITY_OLD_RULEENGINE);
        if (StringUtil.isNotEmpty(isCompatibilityOldRuleEngine)) {
            this.isCompatibilityOldRuleEngine = true;
        }
        if (StringUtil.isNotEmpty(tableName)) {
            this.tableName = tableName;
        }
        LOG.info(
            "Properties resource ,the info is: driver:" + this.jdbcdriver + ",url:" + this.url + ",username:" + userName
                + ",password:" + password);
    }

    @Override
    protected void insertConfigurable(IConfigurable configurable) {
        saveOrUpdate(configurable);
    }

    @Override
    protected void updateConfigurable(IConfigurable configurable) {
        saveOrUpdate(configurable);
    }

    @Override
    public <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type) {

        List<Configure> configures = queryConfigureByNamespaceInner(type, namespace);
        List<IConfigurable> configurables = convert(configures);
        List<T> result = new ArrayList<>();
        for (IConfigurable configurable : configurables) {
            result.add((T)configurable);
        }
        return result;
    }
}
