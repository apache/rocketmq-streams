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
package org.apache.rocketmq.streams.dim.builder;

import com.google.auto.service.AutoService;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.dim.model.DBDim;

@AutoService(IDimSQLParser.class)
@ServiceName(value = DBDimSQLParser.TYPE, aliasName = "rds")
public class DBDimSQLParser extends AbstractDimParser {
    public static final String TYPE = "db";

    @Override protected AbstractDim createDim(Properties properties, MetaData metaData) {
        String tableName = properties.getProperty("tableName");

        /**
         * 创建namelist，要起必须有pco rimary key，，否则抛出错误
         */
        String url = properties.getProperty("url");
        String userName = properties.getProperty("userName");
        String password = properties.getProperty("password");
        String idFieldName = properties.getProperty("idFieldName");

        DBDim dbNameList = new DBDim();
        dbNameList.setUrl(url);
        dbNameList.setUserName(userName);
        dbNameList.setPassword(password);
        dbNameList.setIdFieldName(idFieldName);

        String selectFields = createSelectFields(metaData);
        String sql = "select " + selectFields + " from " + tableName;
        if (tableName.trim().toLowerCase().startsWith("from")) {
            sql = "select " + selectFields + " " + tableName;
        }

        dbNameList.setSql(sql);
        dbNameList.setSupportBatch(true);

        return dbNameList;
    }

    /**
     * 根据字段名，创建sql，最大加载10 w条数据，超过10w会被截断
     *
     * @param metaData
     * @return
     */
    protected String createSelectFields(MetaData metaData) {
        List<MetaDataField> metaDataFields = metaData.getMetaDataFields();
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (MetaDataField field : metaDataFields) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(",");
            }
            stringBuilder.append(field.getFieldName());
        }
        String fields = stringBuilder.toString();
        return fields;
    }

}
