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
package org.apache.rocketmq.streams.db.sink.sqltemplate;

import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.utils.SQLUtil;


public class MysqlInsertIntoWithDuplicateKeySqlTemplate extends MysqlInsertIntoSqlTemplate {

    String sqlSuffix;

    public MysqlInsertIntoWithDuplicateKeySqlTemplate(MetaData metaData, boolean isContainsId) {
        super(metaData, isContainsId);
    }

    @Override
    public void initSqlTemplate() {
        if (sqlPrefix == null) {
            sqlPrefix = SQLUtil.createInsertSegment(metaData, isContainsId);
        }
        if (sqlSuffix == null) {
            sqlSuffix = SQLUtil.createDuplicateKeyUpdateSegment(metaData, isContainsId);
        }
    }

    @Override
    public String createSql(List<? extends Map<String, Object>> rows) {
        initSqlTemplate();
        return String.join(" ", sqlPrefix, SQLUtil.createValuesSegment(metaData, rows, isContainsId), sqlSuffix);
    }

}
