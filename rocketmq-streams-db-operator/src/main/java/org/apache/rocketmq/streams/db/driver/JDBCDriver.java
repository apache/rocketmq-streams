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
package org.apache.rocketmq.streams.db.driver;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.dboperator.IDBDriver;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * 数据库常用操作的封装，核心实现的接口是IJdbcTemplate 这个对象实现了IConfigurable接口，可以序列化存储和网络传输 数据库参数，可以配置成名字，实际值在配置文件配置
 * <p>
 */
public class JDBCDriver extends BasedConfigurable implements IDriverBudiler, IDBDriver {
    private String jdbcDriver = DriverBuilder.DEFALUT_JDBC_DRIVER;
    @ENVDependence
    protected String url;
    @ENVDependence
    protected String userName;
    @ENVDependence
    protected String password;

    protected transient javax.sql.DataSource dataSource;
    private transient IDBDriver dbDriver = null;

    public JDBCDriver(String url, String userName, String password,
                      String driver) {
        setType(ISink.TYPE);
        this.url = url;
        this.userName = userName;
        this.password = password;
        if (StringUtil.isNotEmpty(driver)) {
            this.jdbcDriver = driver;
        }
    }

    public JDBCDriver() {
        setType(ISink.TYPE);
    }

    protected IDBDriver createOrGetDriver() {
        if (dbDriver == null) {
            synchronized (this) {
                if (dbDriver == null) {
                    dbDriver = createDBDriver();
                    if (dataSource == null) {
                        dataSource = createDBDataSource();
                    }
                }
            }
        }
        return dbDriver;
    }

    @Override
    public IDBDriver createDBDriver() {
        javax.sql.DataSource dataSource = createDBDataSource();
        return new IDBDriver() {
            private final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

            @Override
            public int update(String sql) {
                return jdbcTemplate.update(sql);
            }

            @Override
            public void execute(String sql) {
                jdbcTemplate.execute(sql);
            }

            @Override
            public List<Map<String, Object>> queryForList(String sql) {
                return jdbcTemplate.queryForList(sql);
            }

            @Override
            public Map<String, Object> queryOneRow(String sql) {
                return jdbcTemplate.queryForMap(sql);
            }

            @Override
            public long executeInsert(String sql) {
                try {
                    KeyHolder keyHolder = new GeneratedKeyHolder();
                    jdbcTemplate.update(con -> con.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS), keyHolder);
                    if (keyHolder.getKeyList() == null || keyHolder.getKeyList().size() > 1 || keyHolder.getKey() == null) {
                        return 0;
                    }
                    return keyHolder.getKey().longValue();
                } catch (Exception e) {
                    String errorMsg = "execute builder error ,the builder is " + sql + ". the error msg is " + e.getMessage();
                    throw new RuntimeException(errorMsg, e);
                }
            }

            @Override
            public void executSqls(String... sqls) {
                jdbcTemplate.batchUpdate(sqls);
            }

            @Override
            public void executSqls(Collection<String> sqlCollection) {
                if (sqlCollection == null || sqlCollection.size() == 0) {
                    return;
                }
                String[] sqls = new String[sqlCollection.size()];
                int i = 0;
                Iterator<String> it = sqlCollection.iterator();
                while (it.hasNext()) {
                    String sql = it.next();
                    sqls[i] = sql;
                    i++;
                }
                executSqls(sqls);
            }

            /**
             * 分批获取数据，最终获取全量数据
             * @param sql 可执行的SQL
             * @return 结果数据
             */
            @Override
            public List<Map<String, Object>> batchQueryBySql(String sql, int batchSize) {
                List<Map<String, Object>> rows = new ArrayList<>();
                int startBatch;
                String baseSql = sql;
                if (sql.contains(";")) {
                    baseSql = sql.substring(0, sql.indexOf(";"));
                }
                String batchSQL = baseSql + " limit 0," + batchSize;
                List<Map<String, Object>> batchResult = queryForList(batchSQL);
                int index = 1;
                while (batchResult.size() >= batchSize) {
                    rows.addAll(batchResult);
                    startBatch = batchSize * index;
                    batchSQL = baseSql + " limit " + startBatch + "," + batchSize;
                    batchResult = queryForList(batchSQL);
                    index++;
                }
                rows.addAll(batchResult);

                return rows;
            }
        };
    }

    protected javax.sql.DataSource createDBDataSource() {

        SingleConnectionDataSource dataSource = new SingleConnectionDataSource(url, userName, password, true);

        dataSource.setDriverClassName(jdbcDriver);// add by 林行0221
        // 专有云落地运维中心联调时发现独立打包时必须加这句话，否则会报找不到驱动。(MYSQL驱动包虽然已经打进去了但还要在这里显示指定)
        dataSource.setSuppressClose(true);
        this.dataSource = dataSource;
        return dataSource;
    }

    @Override
    public boolean isValidate() {
        try {
            if (dataSource == null) {
                dataSource = createDBDataSource();
            }
            dataSource.getConnection();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    @Override
    public void destroy() {
        if (dataSource instanceof SingleConnectionDataSource) {
            SingleConnectionDataSource data = (SingleConnectionDataSource)dataSource;
            data.destroy();
        }
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

    @Override
    public int update(String sql) {
        return createOrGetDriver().update(sql);
    }

    @Override
    public void execute(String sql) {
        createOrGetDriver().execute(sql);
    }

    @Override
    public List<Map<String, Object>> queryForList(String sql) {
        return createOrGetDriver().queryForList(sql);
    }

    @Override
    public Map<String, Object> queryOneRow(String sql) {
        return createOrGetDriver().queryOneRow(sql);
    }

    @Override
    public long executeInsert(String sql) {
        return createOrGetDriver().executeInsert(sql);
    }

    @Override
    public void executSqls(String... sqls) {
        createOrGetDriver().executSqls(sqls);
    }

    @Override
    public void executSqls(Collection<String> sqls) {
        createOrGetDriver().executSqls(sqls);
    }

    @Override
    public List<Map<String, Object>> batchQueryBySql(String sql, int batchSize) {
        return createOrGetDriver().batchQueryBySql(sql, batchSize);
    }
}
