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

package org.apache.rocketmq.streams.client;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.configuable.ConfigurableComponent;
import org.apache.rocketmq.streams.configuable.model.Configure;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

/**
 * 数据库的存储，需要配置存储的连接参数，请先完成配置，后执行单元用例 如果未建表，可以通过Configure.createTableSQL() 获取建表语句，创建表后，测试
 */
public class DBDriverTest {
    private String URL = "";
    protected String USER_NAME = "";
    protected String PASSWORD = "";
    protected String TABLE_NAME = "rocketmq_streams_configure_source";

    @Test
    public void testDBConfigurableService() {
        String namespace = "streams.db.configurable";

        //正式使用时，在配置文件配置
        ComponentCreator.getProperties().put(ConfigureFileKey.CONNECT_TYPE, "DB");
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, URL);//数据库连接url
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, USER_NAME);//用户名
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, PASSWORD);//password
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_TABLE_NAME, TABLE_NAME);

        //如果表不存在，创建表
        String sql = (Configure.createTableSQL(TABLE_NAME));
        DriverBuilder.createDriver().execute(sql);
        ConfigurableComponent configurableComponent = ConfigurableComponent.getInstance(namespace);
        configurableComponent.insert(createPerson(namespace));
        configurableComponent.refreshConfigurable(namespace);
        Person person = configurableComponent.queryConfigurable("person", "peronName");
        assertNotNull(person);
    }

    /**
     * 创建configuable对象
     *
     * @param namespace
     * @return
     */
    protected Person createPerson(String namespace) {
        Person person = new Person();
        person.setName("chris");
        person.setAge(18);
        person.setNameSpace(namespace);
        person.setConfigureName("peronName");
        person.setType("person");
        return person;
    }
}
