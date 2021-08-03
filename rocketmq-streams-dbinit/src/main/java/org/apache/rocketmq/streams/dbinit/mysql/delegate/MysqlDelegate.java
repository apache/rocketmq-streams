package org.apache.rocketmq.streams.dbinit.mysql.delegate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;

import java.io.IOException;
import java.net.URL;

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
