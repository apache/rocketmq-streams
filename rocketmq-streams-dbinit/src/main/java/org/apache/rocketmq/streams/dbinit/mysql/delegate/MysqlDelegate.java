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

package org.apache.rocketmq.streams.dbinit.mysql.delegate;

import java.io.IOException;
import java.net.URL;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;

public class MysqlDelegate implements DBDelegate {

    public static final Log LOG = LogFactory.getLog(MysqlDelegate.class);


    @Override
    public void init(String driver, final String url, final String userName,
                     final String password) {
        String[] sqls = loadSqls();
        for (String sql : sqls) {
            ORMUtil.executeSQL(sql, null, driver, url, userName, password);
        }
    }

    @Override
    public void init() {
        String[] sqls = loadSqls();
        for (String sql : sqls) {
            ORMUtil.executeSQL(sql, null);
        }
    }

    private String[] loadSqls() {
        String[] sqls = null;
        URL url = this.getClass().getClassLoader().getResource("tables_mysql_innodb.sql");
        try {
            String tables = FileUtil.loadFileContent(url.openStream());
            sqls = tables.split(";");
            if (LOG.isDebugEnabled()) {
                LOG.debug("Init db sqls : " + tables);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return sqls;
    }

}
