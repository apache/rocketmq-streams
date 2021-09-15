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
package org.apache.rocketmq.streams.db.sink;

import com.google.auto.service.AutoService;
import com.google.common.collect.Lists;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.model.ServiceName;

import java.util.List;
import java.util.Properties;

@AutoService(IChannelBuilder.class)
@ServiceName(DBSinkBuilder.TYPE)
public class DBSinkBuilder implements IChannelBuilder {
    public static final String TYPE = "rds";

    @Override
    public ISink createSink(String namespace, String name, Properties properties, MetaData metaData) {
        DBSink sink = new DBSink();
        sink.setUrl(properties.getProperty("url"));
        sink.setUserName(properties.getProperty("userName"));
        sink.setPassword(properties.getProperty("password"));
        List<MetaDataField> fieldList = metaData.getMetaDataFields();

        List<String> insertFields = Lists.newArrayList();
        List<String> insertValues = Lists.newArrayList();
        List<String> duplicateKeys = Lists.newArrayList();
        fieldList.forEach(field -> {
            String fieldName = field.getFieldName();
            insertFields.add(fieldName);
            insertValues.add("'#{" + fieldName + "}'");
            duplicateKeys.add(fieldName + " = VALUES(" + fieldName + ")");
        });

        String sql = "insert into " + properties.getProperty("tableName") + "(" + String.join(",", insertFields) + ") values (" + String.join(",", insertValues) + ")  ";
        sink.setInsertSQLTemplate(sql);
        sink.setDuplicateSQLTemplate(" on duplicate key update " + String.join(",", duplicateKeys));
        return sink;
    }

    @Override
    public ISource createSource(String namespace, String name, Properties properties, MetaData metaData) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public String getType() {
        return TYPE;
    }

}
