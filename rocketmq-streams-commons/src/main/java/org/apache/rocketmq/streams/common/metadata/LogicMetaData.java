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

import java.util.HashSet;
import java.util.Set;

/**
 * @description logic table meta
 */
public class LogicMetaData extends MetaData{

    String tableNamePattern; // table

    Set<String> tableNames; // table_20210731000000


    public LogicMetaData(){
        tableNames = new HashSet<>();
    }

    public String getTableNamePattern() {
        return tableNamePattern;
    }

    public void setTableNamePattern(String tableNamePattern) {
        this.tableNamePattern = tableNamePattern;
    }


    public LogicMetaData addTableName(String tableName){
        tableNames.add(tableName);
        return this;
    }

    public boolean isContainsTable(String tableName){
        return tableNames.contains(tableName);
    }

    public boolean removeTable(String tableName){
        return tableNames.remove(tableName);
    }
}
