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
}
