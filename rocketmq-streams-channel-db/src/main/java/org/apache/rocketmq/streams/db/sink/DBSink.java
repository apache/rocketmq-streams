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
package org.apache.rocketmq.streams.db.sink;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

/**
 * 主要用于写db，输入可以是一个insert/replace 模版，也可以是metadata对象，二者选一即可。都支持批量插入，提高吞吐 sql 模版：insert into table(column1,column2,column3)values('#{var1}',#{var2},'#{var3}') MetaData:主要是描述每个字段的类型，是否必须 二者选一个即可。sql模式，系统会把一批（batchSize）数据拼成一个大sql。metadata模式，基于字段描述，最终也是拼成一个大sql
 */
public class DBSink extends AbstractSink {

    protected String insertSQLTemplate;//完成插入部分的工作，和metadata二选一。insert into table(column1,column2,column3)values('#{var1}',#{var2},'#{var3}')

    protected String duplicateSQLTemplate; //通过on duplicate key update 来对已经存在的信息进行更新

    protected MetaData metaData;//可以指定meta data，和insertSQL二选一

    protected String tableName; //指定要插入的数据表

    @ENVDependence
    protected String jdbcDriver = AbstractComponent.DEFAULT_JDBC_DRIVER;
    @ENVDependence
    protected String url;
    @ENVDependence
    protected String userName;
    @ENVDependence
    protected String password;

    protected boolean openSqlCache=false;

    protected transient IMessageCache<String> sqlCache;//cache sql, batch submit sql

    /**
     * db串多数是名字，可以取个名字前缀，如果值为空，默认为此类的name，name为空，默认为简单类名
     *
     * @param insertSQL        sql模版
     * @param dbInfoNamePrefix 参数可以是名字，这个是名字前缀.真实值可以配置在配置文件中
     */
    public DBSink(String insertSQL, String dbInfoNamePrefix) {
        setType(IChannel.TYPE);
        if (StringUtil.isEmpty(dbInfoNamePrefix)) {
            dbInfoNamePrefix = getConfigureName();
        }
        if (StringUtil.isEmpty(dbInfoNamePrefix)) {
            dbInfoNamePrefix = this.getClass().getSimpleName();
        }
        this.insertSQLTemplate = insertSQL;
        this.url = dbInfoNamePrefix + ".url";
        this.password = dbInfoNamePrefix + ".password";
        this.userName = dbInfoNamePrefix + ".userName";
    }

    public DBSink() {
        setType(IChannel.TYPE);
    }

    public DBSink(String url, String userName, String password) {
        setType(IChannel.TYPE);
        this.url = url;
        this.userName = userName;
        this.password = password;
    }

    public DBSink(String insertSQL, String url, String userName, String password) {
        setType(IChannel.TYPE);
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.insertSQLTemplate = insertSQL;
    }

    @Override
    protected boolean initConfigurable() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            if (StringUtil.isNotEmpty(this.tableName)) {
                Connection connection = DriverManager.getConnection(url, userName, password);
                DatabaseMetaData metaData = connection.getMetaData();
                ResultSet metaResult = metaData.getColumns(connection.getCatalog(), "%", this.tableName, null);
                this.metaData = MetaData.createMetaData(metaResult);
                this.metaData.setTableName(this.tableName);
            }
            sqlCache = new MessageCache<>(new IMessageFlushCallBack<String>() {
                @Override
                public boolean flushMessage(List<String> sqls) {
                    JDBCDriver dataSource = DriverBuilder.createDriver(jdbcDriver, url, userName, password);
                    try {
                        dataSource.executSqls(sqls);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    } finally {
                        if (dataSource != null) {
                            dataSource.destroy();
                        }
                    }
                    return true;
                }
            });
            ((MessageCache<String>) sqlCache).setAutoFlushTimeGap(100000);
            ((MessageCache<String>) sqlCache).setAutoFlushSize(50);
            sqlCache.openAutoFlush();
            return super.initConfigurable();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    protected boolean batchInsert(List<IMessage> messageList) {
        JDBCDriver dbDataSource = DriverBuilder.createDriver(jdbcDriver, url, userName, password);
        try {
            if (messageList == null || messageList.size() == 0) {
                return true;
            }
            List<JSONObject> messages = convertJsonObjectFromMessage(messageList);
            if (StringUtil.isEmpty(insertSQLTemplate) && metaData != null) {
                String sql = SQLUtil.createInsertSql(metaData, messages.get(0));
                sql += SQLUtil.createInsertValuesSQL(metaData, messages.subList(1, messages.size()));
                sql += this.duplicateSQLTemplate;
                executeSQL(dbDataSource, sql);
                return true;
            }
            String insertValueSQL = parseInsertValues(insertSQLTemplate);
            if (StringUtil.isEmpty(insertValueSQL) || insertSQLTemplate.replace(insertValueSQL, "").contains("#{")) {
                for (JSONObject message : messages) {
                    String sql = parseSQL(message, insertSQLTemplate);
                    sql += this.duplicateSQLTemplate;
                    executeSQL(dbDataSource, sql);
                }
                return true;
            } else {
                List<String> subInsert = Lists.newArrayList();
                for (JSONObject message : messages) {
                    subInsert.add(parseSQL(message, insertValueSQL));
                }
                String insertSQL = this.insertSQLTemplate.replace(insertValueSQL, String.join(",", subInsert));
                insertSQL += this.duplicateSQLTemplate;
                executeSQL(dbDataSource, insertSQL);
                return true;
            }
        } finally {
            dbDataSource.destroy();
        }
    }

    @Override
    public boolean checkpoint(Set<String> splitIds) {
        if (sqlCache != null) {
            sqlCache.flush(splitIds);
        }
        return true;
    }

    protected void executeSQL(JDBCDriver dbDataSource, String sql) {
        if (isOpenSqlCache()) {
            this.sqlCache.addCache(sql);
        } else {
            dbDataSource.execute(sql);
        }

    }

    /**
     * 解析出insert value数据部分，对于批量的插入，效果会更佳
     */
    private static final String VALUES_NAME = "values";

    protected String parseInsertValues(String insertSQL) {
        int start = insertSQL.toLowerCase().indexOf(VALUES_NAME);
        if (start == -1) {
            return null;
        }
        String valuesSQL = insertSQL.substring(start + VALUES_NAME.length());
        int end = valuesSQL.toLowerCase().lastIndexOf(")");
        if (end == -1) {
            return null;
        }
        return valuesSQL.substring(0, end + 1);
    }

    protected String parseSQL(JSONObject message, String sql) {
        return SQLUtil.parseIbatisSQL(message, sql);
    }

    public String getInsertSQLTemplate() {
        return insertSQLTemplate;
    }

    public void setInsertSQLTemplate(String insertSQLTemplate) {
        this.insertSQLTemplate = insertSQLTemplate;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isOpenSqlCache() {
        return openSqlCache;
    }

    public void setOpenSqlCache(boolean openSqlCache) {
        this.openSqlCache = openSqlCache;
    }

    public String getDuplicateSQLTemplate() {
        return duplicateSQLTemplate;
    }

    public void setDuplicateSQLTemplate(String duplicateSQLTemplate) {
        this.duplicateSQLTemplate = duplicateSQLTemplate;
    }
}
