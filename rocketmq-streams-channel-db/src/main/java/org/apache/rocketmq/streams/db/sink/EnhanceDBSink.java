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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.ChangeTableNameMessage;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataUtils;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.db.sink.sqltemplate.ISqlTemplate;
import org.apache.rocketmq.streams.db.sink.sqltemplate.SqlTemplateFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @description enhance db sink, support atomic sink and multiple sink
 */
public class EnhanceDBSink extends AbstractSink {

    static final Logger LOGGER = LoggerFactory.getLogger(EnhanceDBSink.class);
    protected MetaData metaData;//可以指定meta data，和insertSQL二选一
    protected String tableName; //指定要插入的数据表
    protected boolean isContainsId = false;
    protected boolean openSqlCache = true;
    /**
     * for atomic sink. default is null
     */
    protected String tmpTableName;
    @ENVDependence
    protected String jdbcDriver = ConfigurationKey.DEFAULT_JDBC_DRIVER;
    @ENVDependence
    protected String url;
    @ENVDependence
    protected String userName;
    @ENVDependence
    protected String password;
    @ENVDependence
    protected String sqlMode;
    protected transient IMessageCache<String> sqlCache;//cache sql, batch submit sql
    protected transient ISqlTemplate iSqlTemplate;
    boolean isAtomic = false; //是否原子写入
    boolean isMultiple = false; //是否多表

    public EnhanceDBSink() {
        this(null, null, null, null);
    }

    public EnhanceDBSink(String url, String userName, String password, String tableName) {
        this(url, userName, password, tableName, ISqlTemplate.SQL_MODE_DEFAULT);
    }

    public EnhanceDBSink(String url, String userName, String password, String tableName, String sqlMode) {
        this(url, userName, password, tableName, sqlMode, null);
    }

    public EnhanceDBSink(String url, String userName, String password, String tableName, String sqlMode, MetaData metaData) {
        setType(IChannel.TYPE);
        this.url = url;
        this.userName = userName;
        this.password = password;
        this.tableName = tableName;
        this.sqlMode = sqlMode;
        this.metaData = metaData;
    }

    @Override
    protected boolean initConfigurable() {

        if (isAtomic && isMultiple) {
            String errMsg = String.format("atomic is not support multiple.");
            LOGGER.error(errMsg);
            throw new RuntimeException(errMsg);
        }
        //如果是多表, 根据逻辑表名创建分区表
        if (isMultiple) {
            createMultiTable();
        }

        //如果是原子写入,根据结果表创建临时表
        if (isAtomic) {
            createTmpTable();
        }

        //如果未设置metadata, 则从db搜索元数据, 创建metadata
        if (metaData == null) {
            createMetaData();
        }

        if (iSqlTemplate == null) {
            try {
                iSqlTemplate = SqlTemplateFactory.newSqlTemplate(sqlMode, metaData, isContainsId);
            } catch (Exception e) {
                LOGGER.error("get sql template error", e);
            }
        }

        if (openSqlCache) {
            initSqlCache();
        }
        return super.initConfigurable();
    }

    private void initSqlCache() {
        this.sqlCache = new MessageCache<>(sqls -> {
            JDBCDriver dataSource = DriverBuilder.createDriver(jdbcDriver, url, userName, password);
            try {
                dataSource.executeSqls(sqls);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                dataSource.destroy();
            }
            return true;
        });
        ((MessageCache<String>) this.sqlCache).setAutoFlushTimeGap(100000);
        ((MessageCache<String>) this.sqlCache).setAutoFlushSize(50);
        this.sqlCache.openAutoFlush();
    }

    /**
     * create
     */
    private void createMetaData() {
        String realInsertTableName = isAtomic ? tmpTableName : tableName;
        metaData = MetaDataUtils.createMetaData(url, userName, password, realInsertTableName);
    }

    private void createMultiTable() {
        String logicTable = subStrLogicTableName(tableName);
        copyAndCreateTableSchema(logicTable, tableName);
    }

    private void createTmpTable() {
        String tmpTable = createTmpTableName(tableName);
        copyAndCreateTableSchema(tableName, tmpTable);

    }

    private void copyAndCreateTableSchema(String sourceTableName, String targetTableName) {
        List<String> tables = MetaDataUtils.listTableNameByPattern(url, userName, password, targetTableName);
        if (tables == null || tables.size() == 0) {
            String createTableSql = getCreateTableSqlFromOther(sourceTableName, tableName);
            createTable(createTableSql);
        }
    }

    private final String getCreateTableSqlFromOther(String sourceTableName, String targetTableName) {
        String createTableSql = MetaDataUtils.getCreateTableSqlByTableName(url, userName, password, sourceTableName);
        createTableSql = createTableSql.replace(sourceTableName, targetTableName);
        LOGGER.info(String.format("createTableSql is %s", createTableSql));
        return createTableSql;

    }

    private final String subStrLogicTableName(String realTableName) {
        int len = realTableName.lastIndexOf("_");
        String logicTableName = realTableName.substring(0, len);
        return logicTableName;
    }

    private final String createTmpTableName(String tableName) {
        return "tmp" + "_" + tableName;
    }

    /**
     * @param createTableSql
     */
    private final void createTable(String createTableSql) {
        ORMUtil.executeSQL(url, userName, password, createTableSql, null);
    }

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        String execSql = genInsertSql(messages);
        executeSQL(execSql);
        return false;
    }

    private String genInsertSql(List<IMessage> messages) {
        String sql = iSqlTemplate.createSql(convertJsonObjectFromMessage(messages));
        return sql;
    }

    @Override
    protected List<JSONObject> convertJsonObjectFromMessage(List<IMessage> messageList) {
        List<JSONObject> messages = new ArrayList<>();
        for (IMessage message : messageList) {
            messages.add(message.getMessageBody());
        }
        return messages;
    }

    protected void executeSQL(String sql) {
        if (isOpenSqlCache()) {
            this.sqlCache.addCache(sql);
        } else {
            JDBCDriver dbDataSource = DriverBuilder.createDriver(jdbcDriver, url, userName, password);
            try {
                dbDataSource.execute(sql);
            } finally {
                dbDataSource.destroy();
            }
        }
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

    public boolean isAtomic() {
        return isAtomic;
    }

    public void setAtomic(boolean atomic) {
        isAtomic = atomic;
    }

    public boolean isMultiple() {
        return isMultiple;
    }

    public void setMultiple(boolean multiple) {
        isMultiple = multiple;
    }

    public boolean isContainsId() {
        return isContainsId;
    }

    public void setContainsId(boolean containsId) {
        isContainsId = containsId;
    }

    public boolean isOpenSqlCache() {
        return openSqlCache;
    }

    public void setOpenSqlCache(boolean openSqlCache) {
        this.openSqlCache = openSqlCache;
    }

    public String getTmpTableName() {
        return tmpTableName;
    }

    public void setTmpTableName(String tmpTableName) {
        this.tmpTableName = tmpTableName;
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

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public void setJdbcDriver(String jdbcDriver) {
        this.jdbcDriver = jdbcDriver;
    }

    public void rename(String suffix) {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        String rename1 = String.format("rename table %s to %s", tableName, tmpTableName.replace("tmp_", "re_") + "_" + suffix + "_" + format.format(new Date()));
        String rename2 = String.format("rename table %s to %s", tmpTableName, tableName);
        LOGGER.info(String.format("exec rename1 %s", rename1));
        LOGGER.info(String.format("exec rename2 %s", rename2));
        ORMUtil.executeSQL(rename1, null);
        ORMUtil.executeSQL(rename2, null);

    }

    @Override
    public void atomicSink(ISystemMessage iMessage) {
        if (isAtomic) {
            ChangeTableNameMessage message = (ChangeTableNameMessage) iMessage;
            rename(message.getScheduleCycle());
            try {
                super.finish();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
