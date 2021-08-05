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
package org.apache.rocketmq.streams.db.driver.orm;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IFieldProcessor;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;

/**
 * 轻量级的orm框架，如果pojo和table 符合驼峰的命名和下划线的命名规范，可以自动实现对象的orm
 */
public class ORMUtil {
    private static final Log LOG = LogFactory.getLog(ORMUtil.class);

    public static <T> T queryForObject(String sql, Object paras, Class<T> convertClass) {
        return queryForObject(sql, paras, convertClass, null, null, null);
    }

    /**
     * 通过sql查询一个唯一的对象出来，返回数据多于一条会报错
     *
     * @param sql                                                                查询语句
     * @param paras                                                              如果有变参，这里面有变参的参数，可以是map，json或对象，只要key名或字段名和sql的参数名相同即可
     * @param convertClass，如果有变参，这里面有变参的参数，可以是map，json或对象，只要key名或字段名和sql的参数名相同即可
     * @param url                                                                数据库连接url
     * @param userName                                                           用户名
     * @param password                                                           密码
     * @param <T>
     * @return 转换后的对象
     */
    public static <T> T queryForObject(String sql, Object paras, Class<T> convertClass, String url, String userName,
                                       String password) {
        List<T> result = queryForList(sql, paras, convertClass, url, userName, password);
        if (result == null || result.size() == 0) {
            return null;
        }
        if (result.size() > 1) {
            throw new RuntimeException("expect only one row, actual " + result.size() + ". the builder is " + sql);
        }
        return result.get(0);
    }

    /**
     * 根据sql查询一批对象
     *
     * @param sql          查询语句
     * @param paras        如果有变参，这里面有变参的参数，可以是map，json或对象，只要key名或字段名和sql的参数名相同即可
     * @param convertClass 如果有变参，这里面有变参的参数，可以是map，json或对象，只要key名或字段名和sql的参数名相同即可
     * @param <T>
     * @return 返回对象列表
     */
    public static <T> List<T> queryForList(String sql, Object paras, Class<T> convertClass) {
        return queryForList(sql, paras, convertClass, null, null, null);
    }

    public static boolean hasConfigueDB() {
        return ComponentCreator.getProperties().getProperty(ConfigureFileKey.JDBC_URL) != null;
    }

    /**
     * @param sql          查询语句
     * @param paras        如果有变参，这里面有变参的参数，可以是map，json或对象，只要key名或字段名和sql的参数名相同即可
     * @param convertClass 需要转换成的对象类。类的字段应该符合列名的命名规范，下划线换成驼峰形式
     * @param url          数据库连接url
     * @param userName     用户名
     * @param password     密码
     * @param <T>
     * @return 返回对象列表
     */
    public static <T> List<T> queryForList(String sql, Object paras, Class<T> convertClass, String url, String userName,
                                           String password) {
        sql = SQLUtil.parseIbatisSQL(paras, sql);
        JDBCDriver dataSource = null;
        try {
            if (StringUtil.isEmpty(url) || StringUtil.isEmpty(userName)) {
                dataSource = DriverBuilder.createDriver();
            } else {
                dataSource = DriverBuilder.createDriver(DriverBuilder.DEFALUT_JDBC_DRIVER, url, userName, password);
            }

            List<Map<String, Object>> rows = dataSource.queryForList(sql);
            List<T> result = new ArrayList();
            for (Map<String, Object> row : rows) {
                T t = convert(row, convertClass);
                result.add(t);
            }
            return result;
        } catch (Exception e) {
            String errorMsg = ("query for list  error ,the builder is " + sql + ". the error msg is " + e.getMessage());
            LOG.error(errorMsg);
            e.printStackTrace();
            throw new RuntimeException(errorMsg, e);
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }

    /**
     * 执行sql，sql中可以有mybatis的参数#{name}
     *
     * @param sql   insert语句
     * @param paras 可以是map，json或对象，只要key名或字段名和sql的参数名相同即可
     * @return
     */
    public static boolean executeSQL(String sql, Object paras) {
        if (paras != null) {
            sql = SQLUtil.parseIbatisSQL(paras, sql);
        }
        JDBCDriver dataSource = null;
        try {
            dataSource = DriverBuilder.createDriver();
            dataSource.execute(sql);
            return true;
        } catch (Exception e) {
            String errorMsg = ("execute sql  error ,the sql is " + sql + ". the error msg is " + e.getMessage());
            LOG.error(errorMsg);
            e.printStackTrace();
            throw new RuntimeException(errorMsg, e);
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }


    public static boolean executeSQL(String sql, Object paras, String driver, final String url, final String userName,
                                     final String password) {
        if (paras != null) {
            sql = SQLUtil.parseIbatisSQL(paras, sql);
        }
        JDBCDriver dataSource = null;
        try {
            dataSource = DriverBuilder.createDriver(driver, url, userName, password);
            dataSource.execute(sql);
            return true;
        } catch (Exception e) {
            String errorMsg = ("execute sql  error ,the sql is " + sql + ". the error msg is " + e.getMessage());
            LOG.error(errorMsg);
            e.printStackTrace();
            throw new RuntimeException(errorMsg, e);
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }

    /**
     * 把一个对象的字段拼接成where条件，如果字段值为null，不拼接
     *
     * @param object     带拼接的对象
     * @param fieldNames 需要拼接的字段名，如果为null，返回null
     * @return where 部分的sql
     */
    public static String createQueryWhereSql(Object object, String... fieldNames) {
        if (fieldNames == null || fieldNames.length == 0) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (String fieldName : fieldNames) {
            Object value = ReflectUtil.getBeanFieldValue(object, fieldName);
            if (object != null && value == null) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(" and ");
            }
            String columnName = getColumnNameFromFieldName(fieldName);
            stringBuilder.append(" " + columnName + "=#{" + fieldName + "} ");
        }
        return stringBuilder.toString();
    }

    /**
     * 把一行数据转换成一个对象，符合驼峰的命名和下划线的命名规范
     *
     * @param row   一行数据
     * @param clazz 待转化的对象类型
     * @param <T>
     * @return 转化对象
     */
    public static <T> T convert(Map<String, Object> row, Class<T> clazz) {
        T t = ReflectUtil.forInstance(clazz);
        Iterator<Map.Entry<String, Object>> it = row.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String columnName = entry.getKey();
            Object value = entry.getValue();
            if (value == null) {
                continue;
            }
            String fieldName = getFieldNameFromColumnName(columnName);
            DataType datatype = DataTypeUtil.createFieldDataType(clazz, fieldName);
            Object columnValue = datatype.convert(value);
            ReflectUtil.setBeanFieldValue(t, fieldName, columnValue);
        }
        return t;
    }

    /**
     * 把列名转换成字段名称，把下划线转化成驼峰
     *
     * @param columnName
     * @return
     */
    protected static String getFieldNameFromColumnName(String columnName) {
        String[] values = columnName.split("_");
        if (values.length == 1) {
            return columnName;
        }
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(values[0]);
        for (int i = 1; i < values.length; i++) {
            String value = values[i];
            value = value.substring(0, 1).toUpperCase() + value.substring(1);
            stringBuilder.append(value);
        }
        return stringBuilder.toString();
    }

    /**
     * 对象批量替换，会生成replace into语句，多个对象会拼接成一个sql，提升效率
     *
     * @param values 待插入对象
     */
    public static void batchReplaceInto(Collection values) {
        List list = new ArrayList<>();
        list.addAll(values);
        batchReplaceInto(list);
    }

    public static void batchReplaceInto(Object... valueArray) {
        List values = new ArrayList();
        if (valueArray != null) {
            for (Object value : valueArray) {
                values.add(value);
            }
        }
        batchReplaceInto(values);
    }

    public static void batchReplaceInto(List<?> values) {
        batchIntoByFlag(values, 1);
    }

    /**
     * 对象批量插入，如果主键冲突会忽略，会生成insert ignore into语句，多个对象会拼接成一个sql，提升效率
     *
     * @param values 待插入对象
     */
    public static void batchIgnoreInto(List<?> values) {
        batchIntoByFlag(values, -1);
    }

    /**
     * 对象批量插入，如果主键冲突会忽略，会生成insert ignore into语句，多个对象会拼接成一个sql，提升效率
     *
     * @param values 待插入对象
     */
    public static void batchInsertInto(List<?> values) {
        batchIntoByFlag(values, 0);
    }

    /**
     * 批量插入对象，多个对象会拼接成一个sql flag==1 then replace into flag=-1 then insert ignore int flag=0 then insert int
     *
     * @param values
     * @param flag
     */
    protected static void batchIntoByFlag(List<?> values, int flag) {
        if (CollectionUtil.isEmpty(values)) {
            return;
        }
        Object object = values.get(0);
        Map<String, Object> paras = new HashMap<>(16);
        MetaData metaData = createMetaDate(object, paras);
        boolean containsIdField = false;
        if (metaData.getIdFieldName() != null) {
            for (Object o : values) {
                Object id = ReflectUtil.getDeclaredField(o, metaData.getIdFieldName());
                if (id == null) {
                    containsIdField = false;
                    break;
                }
                if (id instanceof Number) {
                    if (Long.valueOf(id.toString()) == 0) {
                        containsIdField = false;
                        break;
                    }
                }
                if (id instanceof String) {
                    String idStr = (String)id;
                    if (StringUtil.isEmpty(idStr)) {
                        containsIdField = false;
                        break;
                    }
                }
            }
        }

        String sql = null;
        if (flag == 0) {
            sql = SQLUtil.createInsertSql(metaData, paras, containsIdField);
        } else if (flag == 1) {
            sql = SQLUtil.createInsertSql(metaData, paras, containsIdField);
        } else if (flag == -1) {
            sql = SQLUtil.createIgnoreInsertSql(metaData, paras, containsIdField);
        } else {
            throw new RuntimeException("the flag is not valdate " + flag);
        }

        List<Map<String, Object>> rows = new ArrayList<>();
        for (int i = 1; i < values.size(); i++) {
            Map<String, Object> row = createRow(metaData, values.get(i));
            rows.add(row);
        }
        String valuesSQL = SQLUtil.createInsertValuesSQL(metaData, rows);
        sql = sql + valuesSQL + " ON DUPLICATE  KEY  UPDATE " + SQLUtil.createDuplicateKeyUpdateSQL(metaData);
        ;
        JDBCDriver dataSource = DriverBuilder.createDriver();
        try {
            dataSource.execute(sql);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }

    private static Map<String, Object> createRow(MetaData metaData, Object object) {
        Map<String, Object> row = new HashMap<>();
        ReflectUtil.scanFields(object, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                String fieldName = field.getName();
                String columnName = getColumnNameFromFieldName(fieldName);
                Object value = ReflectUtil.getBeanFieldValue(o, fieldName);
                row.put(columnName, value);

            }
        });
        return row;
    }

    public static void replaceInto(Object object) {
        replaceInto(object, null, null, null);
    }

    /**
     * 把一个对象插入到数据库，对象符合插入规范，表名是对象名转小写后加下划线。如果有重复到会被替换成最新的
     *
     * @param object
     * @param url
     * @param userName
     * @param password
     */
    public static void replaceInto(Object object, String url, String userName, String password) {
        Map<String, Object> paras = new HashMap<>();
        if (Entity.class.isInstance(object)) {
            Entity newEntity = (Entity)object;
            newEntity.setGmtModified(new Date());
            if (newEntity.getGmtCreate() == null) {
                newEntity.setGmtCreate(new Date());
            }
        }
        MetaData metaData = createMetaDate(object, paras);
        String sql = SQLUtil.createReplacesInsertSql(metaData, paras, metaData.getIdFieldName() != null);
        JDBCDriver dataSource = null;
        try {
            if (StringUtil.isEmpty(url) || StringUtil.isEmpty(userName)) {
                dataSource = DriverBuilder.createDriver();
            } else {
                dataSource = DriverBuilder.createDriver(DriverBuilder.DEFALUT_JDBC_DRIVER, url, userName, password);
            }
            long id = dataSource.executeInsert(sql);
            if (Entity.class.isInstance(object)) {
                Entity newEntity = (Entity)object;
                newEntity.setId(id);
            }
        } catch (Exception e) {
            String errorMsg = ("replace into error ,the builder is " + sql + ". the error msg is " + e.getMessage());
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg, e);
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }

    public static void insertInto(Object object, boolean ignoreRepeateRow) {
        insertInto(object, ignoreRepeateRow, null, null, null);
    }

    /**
     * 把一个对象插入到数据库，对象符合插入规范，表名是对象名转小写后加下划线
     *
     * @param object
     * @param ignoreRepeateRow，如果是重复数据，则不插入。基于唯一建做判断
     * @param url
     * @param userName
     * @param password
     */
    public static void insertInto(Object object, boolean ignoreRepeateRow, String url, String userName,
                                  String password) {
        Map<String, Object> paras = new HashMap<>();
        if (Entity.class.isInstance(object)) {
            Entity newEntity = (Entity)object;
            newEntity.setGmtCreate(DateUtil.getCurrentTime());
            newEntity.setGmtModified(DateUtil.getCurrentTime());
        }
        MetaData metaData = createMetaDate(object, paras);
        String sql = null;
        if (ignoreRepeateRow) {
            sql = SQLUtil.createIgnoreInsertSql(metaData, paras, metaData.getIdFieldName() != null);
        } else {
            sql = SQLUtil.createInsertSql(metaData, paras, metaData.getIdFieldName() != null);
        }
        JDBCDriver dataSource = null;
        try {
            if (StringUtil.isEmpty(url) || StringUtil.isEmpty(userName)) {
                dataSource = DriverBuilder.createDriver();
            } else {
                dataSource = DriverBuilder.createDriver(DriverBuilder.DEFALUT_JDBC_DRIVER, url, userName, password);
            }
            long id = dataSource.executeInsert(sql);
            if (Entity.class.isInstance(object)) {
                Entity newEntity = (Entity)object;
                newEntity.setId(id);
            }
        } catch (Exception e) {
            String errorMsg = ("insert into error ,the builder is " + sql + ". the error msg is " + e.getMessage());
            LOG.error(errorMsg);
            throw new RuntimeException(errorMsg, e);
        } finally {
            if (dataSource != null) {
                dataSource.destroy();
            }
        }
    }

    /**
     * 创建meta信息
     *
     * @param object
     * @param paras
     * @return
     */
    public static MetaData createMetaDate(Object object, Map<String, Object> paras) {
        MetaData metaData = MetaData.createMetaDate(object, paras);
        return metaData;
    }

    public static String getTableName(Class clazz) {
        return getColumnNameFromFieldName(clazz.getSimpleName());
    }

    /**
     * 把驼峰转换成下划线的形式
     *
     * @param para
     * @return
     */
    protected static String getColumnNameFromFieldName(String para) {
        return MetaData.getColumnNameFromFieldName(para);
    }

}
