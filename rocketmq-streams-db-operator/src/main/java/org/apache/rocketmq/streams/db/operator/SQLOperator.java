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
package org.apache.rocketmq.streams.db.operator;

import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.Changeable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.NewSQLChainStage;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;

/**
 * sql算法，执行一个sql，sql中可以有变量，会用message的值做替换。
 */
public class SQLOperator extends BasedConfigurable implements IStreamOperator<IMessage, IMessage>, IStageBuilder<ChainStage> {
    public static final String DEFALUT_DATA_KEY = "data";

    @ENVDependence
    protected String jdbcDriver = AbstractComponent.DEFAULT_JDBC_DRIVER;
    @ENVDependence
    protected String url;
    @ENVDependence
    protected String userName;
    @ENVDependence
    protected String password;

    @Changeable
    protected String sql;//查询的sql，支持ibatis的语法和变量.因为会被替换，所以不自动感知。select * from table where name=#{name=chris}

    public SQLOperator() {
        setType(IStreamOperator.TYPE);
    }

    public SQLOperator(String sql, String url, String userName, String password) {
        this();
        this.sql = sql;
        this.url = url;
        this.password = password;
        this.userName = userName;
    }

    /**
     * db串多数是名字，可以取个名字前缀，如果值为空，默认为此类的name，name为空，默认为简单类名
     *
     * @param sql
     * @param dbInfoNamePrex
     */
    public SQLOperator(String sql, String dbInfoNamePrex) {
        this();
        if (StringUtil.isEmpty(dbInfoNamePrex)) {
            dbInfoNamePrex = getConfigureName();
        }
        if (StringUtil.isEmpty(dbInfoNamePrex)) {
            dbInfoNamePrex = this.getClass().getSimpleName();
        }
        this.sql = sql;
        this.url = dbInfoNamePrex + ".url";
        this.password = dbInfoNamePrex + ".password";
        ;
        this.userName = dbInfoNamePrex + ".userName";
    }

    @Override
    public IMessage doMessage(IMessage message, AbstractContext context) {
        String querySQL = SQLUtil.parseIbatisSQL(message.getMessageBody(), sql);
        List<Map<String, Object>> result = query(querySQL);
        message.getMessageBody().put(DEFALUT_DATA_KEY, result);
        return message;
    }

    /**
     * 查询数据库数据
     *
     * @return
     */
    protected List<Map<String, Object>> query(String querySQL) {

        JDBCDriver dataSource = null;
        try {
            dataSource = createDBDataSource();
            List<Map<String, Object>> result = null;
            result = dataSource.queryForList(sql);

            return result;
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
        }

    }

    public JDBCDriver createDBDataSource() {
        return DriverBuilder.createDriver(jdbcDriver, url, userName, password);
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
    }

    @Override
    public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
        NewSQLChainStage sqlChainStage = new NewSQLChainStage();
        sqlChainStage.setMessageProcessor(this);
        return sqlChainStage;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
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

}
