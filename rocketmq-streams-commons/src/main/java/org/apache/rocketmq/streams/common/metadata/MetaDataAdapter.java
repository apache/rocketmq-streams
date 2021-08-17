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

import java.util.Map;
import org.apache.rocketmq.streams.common.dboperator.IDBDriver;
import org.apache.rocketmq.streams.common.utils.SQLUtil;

public class MetaDataAdapter<T> {

    private MetaData metaData;
    private IDBDriver dataSource;

    public MetaDataAdapter(MetaData pmetaData, IDBDriver pdataSource) {
        this.metaData = pmetaData;
        this.dataSource = pdataSource;
    }

    public Boolean insert(Map<String, Object> fieldName2Value) {
        String sql = SQLUtil.createInsertSql(metaData, fieldName2Value);
        dataSource.executeInsert(sql);
        return true;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public IDBDriver getDataSource() {
        return dataSource;
    }

}
