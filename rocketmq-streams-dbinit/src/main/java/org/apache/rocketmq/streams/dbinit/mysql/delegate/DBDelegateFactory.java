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

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;

public class DBDelegateFactory {

    public static DBDelegate getDelegate() {
        String dbType = ComponentCreator.getProperties().getProperty(ConfigureFileKey.DB_TYPE);
        if (dbType == null || "".equalsIgnoreCase(dbType)) {
            dbType = DBType.DB_MYSQL;
        }
        if (DBType.DB_MYSQL.equalsIgnoreCase(dbType)) {
            return new MysqlDelegate();
        }

        return new MysqlDelegate();
    }

    public static DBDelegate getDelegate(String dbType) {
        if (DBType.DB_MYSQL.equalsIgnoreCase(dbType)) {
            return new MysqlDelegate();
        }

        return new MysqlDelegate();
    }

    public static void main(String[] args) {
        DBDelegateFactory.getDelegate().init();
    }

}
