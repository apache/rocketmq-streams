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

import java.util.Arrays;
import org.apache.rocketmq.streams.common.metadata.MetaData;

/**
 * @description
 */
public class SqlTemplateFactory {

    public static ISqlTemplate newSqlTemplate(String type, MetaData metaData, boolean isContainsId) throws Exception {

        if(ISqlTemplate.SQL_MODE_DEFAULT.equalsIgnoreCase(type)){
            return new MysqlInsertIntoSqlTemplate(metaData, isContainsId);
        }else if(ISqlTemplate.SQL_MODE_DUPLICATE.equalsIgnoreCase(type)){
            return new MysqlInsertIntoWithDuplicateKeySqlTemplate(metaData, isContainsId);
        }else if(ISqlTemplate.SQL_MODE_IGNORE.equalsIgnoreCase(type)){
            return new MysqlInsertIgnoreIntoSqlTemplate(metaData, isContainsId);
        }else{
            throw new Exception(String.format("unsupported type %s, only support %s. ", type, Arrays.toString(ISqlTemplate.SUPPORTS)));
        }
    }

}
