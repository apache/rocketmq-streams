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
package com.aliyun.service;

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.dim.model.DBDim;
import org.junit.Test;

public class DBDimTest {
    @Test
    public void testDBDim() {

        /**
         * udf open执行
         * dbDim可以声明成静态的（相同的url，username，password，sql），同一个进程可以共享
         * 可以通过job参数获取url，username，password
         */
        DBDim dbDim = new DBDim();
        dbDim.setUrl("");
        dbDim.setUserName("");
        dbDim.setPassword("");
        dbDim.setSql("");
        dbDim.setIdFieldName("id");
        dbDim.setPollingTimeSeconds(30L);
        dbDim.addIndex("ip");
        dbDim.init();
        dbDim.startLoadDimData();
        System.out.println("cost memory is " + dbDim.getDataCache().getByteCount() / 1024 / 1024 + "M");

        /**
         * udf eval 执行，可多条匹配
         */
        JSONObject msg = null;
        String msgFieldName = null;
        String dimFieldName = null;

        List<Map<String, Object>> resultList = dbDim.matchExpression(msgFieldName, dimFieldName, msg);
    }

    public void testSelf() {
        new AbstractDim() {

            @Override protected void loadData2Memory(AbstractMemoryTable table) {
                table.addRow(new HashMap<>());

            }
        };
    }
}
