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

import java.lang.reflect.Constructor;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 创建JDBCDriver，如果没有
 */
public class DriverBuilder {

    private static final Log LOG = LogFactory.getLog(DriverBuilder.class);

    public static final String DEFALUT_JDBC_DRIVER = "com.mysql.jdbc.Driver";

    private static final Map<String, JDBCDriver> dataSourceMap = new ConcurrentHashMap<>();

    private static AtomicInteger count = new AtomicInteger(0);

    /**
     * 使用ConfiguableComponent在属性文件配置的jdbc信息，dipper默认都是使用这个数据库连接 如果需要连接其他库，需要使用带参数的createDriver
     *
     * @return
     */
    public static JDBCDriver createDriver() {
        String driver = ComponentCreator.getProperties().getProperty(AbstractComponent.JDBC_DRIVER);
        String url = ComponentCreator.getProperties().getProperty(AbstractComponent.JDBC_URL);
        String userName = ComponentCreator.getProperties().getProperty(AbstractComponent.JDBC_USERNAME);
        String password = ComponentCreator.getProperties().getProperty(AbstractComponent.JDBC_PASSWORD);
        return createDriver(driver, url, userName, password);
    }

    /**
     * 根据数据库连接信息创建连接，并返回JDBCDriver
     *
     * @param driver   数据库驱动，如果为null，默认为mysql
     * @param url      数据库连接url
     * @param userName 用户名
     * @param password 密码
     * @return JDBCDriver
     */
    public static JDBCDriver createDriver(String driver, final String url, final String userName,
                                          final String password) {
        if (StringUtil.isEmpty(driver)) {
            driver = DEFALUT_JDBC_DRIVER;
        }
        String className = ComponentCreator.getDBProxyClassName();
        if (StringUtil.isNotEmpty(className)) {
            Class clazz = ReflectUtil.forClass(className);
            try {
                Constructor constructor = clazz.getConstructor(
                    new Class[] {String.class, String.class, String.class, String.class});
                JDBCDriver abstractDBDataSource = (JDBCDriver)constructor.newInstance(url, userName, password,
                    driver);
                abstractDBDataSource.init();
                return abstractDBDataSource;
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        }

        final String jdbcdriver = driver;
        ReflectUtil.forClass(jdbcdriver);
        JDBCDriver resource = new JDBCDriver();
        LOG.debug("jdbcdriver=" + jdbcdriver + ",url=" + url);
        resource.setJdbcDriver(jdbcdriver);
        resource.setUrl(url);
        resource.setUserName(userName);
        resource.setPassword(password);
        resource.init();
        return resource;
    }

    /**
     * 生成拼接字符串
     *
     * @param url
     * @param userName
     * @param password
     * @return
     */
    private static String genereateKey(String url, String userName, String password) {
        return url + "_" + userName + "_" + password;
    }

}

