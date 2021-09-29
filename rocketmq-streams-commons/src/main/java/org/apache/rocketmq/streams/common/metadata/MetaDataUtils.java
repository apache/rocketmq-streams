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
package org.apache.rocketmq.streams.common.metadata;

import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

import java.sql.*;
import java.util.*;

/**
 * @description metaDataUtils
 */
public class MetaDataUtils {

    private static final String DEFAULT_DRIVER = AbstractComponent.DEFAULT_JDBC_DRIVER;


    /**
     * 去除id字段,主要用于拼接sql
     * @param metaData
     * @return
     */
    public static final MetaData getMetaDataWithOutId(MetaData metaData){

        List<MetaDataField> fields = metaData.getMetaDataFields();
        String id = metaData.getIdFieldName();
        Iterator<MetaDataField> it = fields.iterator();
        while(it.hasNext()){
            MetaDataField field = it.next();
            String fieldName = field.getFieldName();
            if(fieldName.equals(id)){
                it.remove();
                metaData.setMetaDataFields(fields);
                return metaData;
            }
        }
        return metaData;
    }

    /**
     * 创建metadata, 会填充columns列表, 并使用primary key填充idFieldName, 并做了ID名称的限制。
     * @param url
     * @param userName
     * @param password
     * @param tableName
     * @return
     */
    public static final MetaData createMetaData(String url, String userName, String password, String tableName){
        return connectionWrapper(DEFAULT_DRIVER, url, userName, password, new MetaFunction<MetaData>() {
            @Override
            public MetaData function(Connection connection, DatabaseMetaData databaseMetaData, String catalog) throws SQLException {
                ResultSet metaResult = databaseMetaData.getColumns(connection.getCatalog(), "%", tableName, null);
                ResultSet resultSet = databaseMetaData.getPrimaryKeys(catalog, "%", tableName);
                MetaData metaData = MetaData.createMetaData(metaResult);
                if(resultSet.next()){
                    String pkType = resultSet.getString("PK_NAME");
                    String pkName = resultSet.getString("COLUMN_NAME");
                    if("PRIMARY".equalsIgnoreCase(pkType) && "id".equalsIgnoreCase(pkName)){
                        metaData.setIdFieldName(pkName);
                    }
                }
                metaData.setTableName(tableName);
                return metaData;
            }
        });
    }


    public static List<String> listTableNameByPattern(String url, String userName, String password, String tableNamePattern){
        return listTableNameByPattern(DEFAULT_DRIVER, url, userName, password, tableNamePattern);
    }

    /**
     * 根据已知表名获取建表语句
     * @param url
     * @param userName
     * @param password
     * @param tableName
     * @return
     */
    public static String getCreateTableSqlByTableName(String url, String userName, String password, String tableName){
        return connectionWrapper(DEFAULT_DRIVER, url, userName, password, new MetaFunction<String>() {
            @Override
            public String function(Connection connection, DatabaseMetaData databaseMetaData, String catalog) throws SQLException {
                String createTableSql = null;
                String showTableSql = "show create table " + tableName + ";";
                PreparedStatement ps = connection.prepareStatement(showTableSql);
                ResultSet rs = ps.executeQuery();
                while(rs.next()){
                    createTableSql = rs.getString(2);
                }

                return createTableSql;
            }
        });
    }


    public static List<String> listTableNameByPattern(String driver, String url, String userName, String password, String tableNamePattern){

        return connectionWrapper(driver, url, userName, password, new MetaFunction<List<String>>() {
            @Override
            public List<String> function(Connection connection, DatabaseMetaData databaseMetaData, String catalog) throws SQLException {
                ResultSet metaResult = databaseMetaData.getTables(catalog,"%", tableNamePattern, new String[]{"TABLE"});
                if(metaResult.wasNull()){
                    return null;
                }
                List<String> tableNames = new ArrayList<>();
                while(metaResult.next()) {
                    String tableName = metaResult.getString("TABLE_NAME");
                    tableNames.add(tableName);
                }
                return tableNames;
            }
        });
    }

    public static final LogicMetaData createMetaDataByLogicTableName(String url, String userName, String password, String tableNamePattern){
        return createMetaDataByLogicTableName(DEFAULT_DRIVER, url, userName, password, tableNamePattern);
    }


    public static final LogicMetaData createMetaDataByLogicTableName(String driver, String url, String userName, String password, String tableNamePattern){

        return connectionWrapper(driver, url, userName, password,  new MetaFunction<LogicMetaData>() {
            @Override
            public LogicMetaData function(Connection connection, DatabaseMetaData databaseMetaData, String catalog) throws SQLException {

                ResultSet metaResult = databaseMetaData.getTables(catalog,"%", tableNamePattern, new String[]{"TABLE"});

                if(metaResult.wasNull()){
                    return null;
                }

                boolean isFirst = true;
                //key columnName, value columnType
                Map<String, String> columns = new LinkedHashMap<>();
                LogicMetaData metaData = new LogicMetaData();

                while(metaResult.next()){
                    String tableName = metaResult.getString("TABLE_NAME");
                    ResultSet columnSet = databaseMetaData.getColumns(catalog, "%", tableName, null);
                    int len = 0;
                    boolean loop = true;
                    metaData.addTableName(tableName);

                    while(loop && columnSet.next()){
                        len++;
                        String columnName = columnSet.getString("COLUMN_NAME");
                        String columnType = columnSet.getString("TYPE_NAME");

                        if(isFirst){
                            columns.put(columnName, columnType);
                        }else{
                            if(!columnType.equals(columns.get(columnName))){
                                // skip current table columnName get
                                loop = false;
                                metaData.removeTable(tableName);
                                throw new RuntimeException(String.format("table %s fieldName is difference with column set %s, current fieldName is %s, skip the current table meta check", tableName, Arrays.toString(columns.keySet().toArray()), columnName));
                            }
                        }
                    }
                    isFirst = false;
                    if(len != columns.size()){
                        throw new RuntimeException(String.format("table length is difference with column set, tableName is %s, length is %d, column size is %d", tableName, len, columns.size()));
                    }
                }

                metaData.setTableNamePattern(tableNamePattern);
                for(Map.Entry<String, String> entry : columns.entrySet()){
                    MetaDataField field = new MetaDataField();
                    field.setFieldName(entry.getKey());
                    field.setDataType(DataTypeUtil.createDataTypeByDbType(entry.getValue()));
                    metaData.getMetaDataFields().add(field);
                }

                return metaData;
            }
        });
    }


//    private static final List<String> get



    private static <T> T connectionWrapper(String driver, String url, String userName, String password, MetaFunction<T> function) {

        Connection connection = null;
        try {
            Class.forName(driver);
            connection = DriverManager.getConnection(url, userName, password);
            String catalog = connection.getCatalog();
            DatabaseMetaData databaseMetaData = connection.getMetaData();
            return function.function(connection, databaseMetaData, catalog);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            return null;
        } catch (SQLException throwables) {
            throwables.printStackTrace();
            return null;
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }

    private interface MetaFunction<T> {

        T function(Connection connection, DatabaseMetaData databaseMetaData, String catalog) throws SQLException;

    }


}
