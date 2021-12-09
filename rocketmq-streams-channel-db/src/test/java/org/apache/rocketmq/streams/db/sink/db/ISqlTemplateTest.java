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
package org.apache.rocketmq.streams.db.sink.db;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.db.sink.sqltemplate.MysqlInsertIgnoreIntoSqlTemplate;
import org.apache.rocketmq.streams.db.sink.sqltemplate.MysqlInsertIntoSqlTemplate;
import org.apache.rocketmq.streams.db.sink.sqltemplate.MysqlInsertIntoWithDuplicateKeySqlTemplate;
import org.junit.Test;

/**
 * @description
 */
public class ISqlTemplateTest {

    static MetaData metaData;

    static List<JSONObject> rows;

    static{
        JSONObject jsonObject1 = new JSONObject();
        jsonObject1.put("id", 1);
        jsonObject1.put("name", "chris");
        jsonObject1.put("age", 18);

        JSONObject jsonObject2 = new JSONObject();
        jsonObject2.put("id", 2);
        jsonObject2.put("name", "tom");
        jsonObject2.put("age", 19);

        JSONObject jsonObject3 = new JSONObject();
        jsonObject3.put("id", 3);
        jsonObject3.put("name", "ken");
        jsonObject3.put("age", 20);
        rows = new ArrayList<>();
        rows.add(jsonObject1);
        rows.add(jsonObject2);
        rows.add(jsonObject3);
        metaData = MetaData.createMetaData(jsonObject1);
        metaData.setTableName("mock_table_name");
        metaData.setIdFieldName("id");
    }

    @Test
    public void testInsertInto(){
        MysqlInsertIntoSqlTemplate templateWithId = new MysqlInsertIntoSqlTemplate(metaData, true);
        System.out.println(templateWithId.createSql(rows));
        MysqlInsertIntoSqlTemplate templateWithOutId = new MysqlInsertIntoSqlTemplate(metaData, false);
        System.out.println(templateWithOutId.createSql(rows));
    }

    @Test
    public void testInsertIgnoreInto(){
        MysqlInsertIgnoreIntoSqlTemplate templateWithId = new MysqlInsertIgnoreIntoSqlTemplate(metaData, true);
        System.out.println(templateWithId.createSql(rows));
        MysqlInsertIgnoreIntoSqlTemplate templateWithOutId = new MysqlInsertIgnoreIntoSqlTemplate(metaData, false);
        System.out.println(templateWithOutId.createSql(rows));
    }

    @Test
    public void testInsertDuplicate(){
        MysqlInsertIntoWithDuplicateKeySqlTemplate templateWithId = new MysqlInsertIntoWithDuplicateKeySqlTemplate(metaData, true);
        System.out.println(templateWithId.createSql(rows));
        MysqlInsertIntoWithDuplicateKeySqlTemplate templateWithOutId = new MysqlInsertIntoWithDuplicateKeySqlTemplate(metaData, false);
        System.out.println(templateWithOutId.createSql(rows));
    }

}
