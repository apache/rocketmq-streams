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
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.cache.compress.KVAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.dim.model.DBDim;
import org.junit.Test;

public class NameListFunctionTest {

    @Test
    public void testInterge() {
        KVAddress mapAddress = new KVAddress(128, 0);
        byte[] bytes = mapAddress.createBytesIngoreFirstBit();
        long rowIndex = NumberUtils.toLong(bytes);
        KVAddress mapAddress1 = KVAddress.createMapAddressFromLongValue(rowIndex);
        System.out.println(rowIndex);
    }

    @Test
    public void testNameList() {
        AbstractDim nameList = create();
        JSONObject msg = new JSONObject();
        msg.put("ip", "47.105.77.144");
        msg.put("vpcId", "1");
        msg.put("now", "2019-07-18 17:33:29");
        long start = System.currentTimeMillis();
    }

    @Test
    public void testNameList2() {
        AbstractDim nameList = createMapping();
        JSONObject msg = new JSONObject();
        msg.put("levelFile", "aegis-vul_record:level");
        msg.put("levelValue", "high");
        msg.put("now", "2019-07-18 17:33:29");
        long start = System.currentTimeMillis();
    }

    private AbstractDim create() {
        DBDim dbNameList = new DBDim();
        dbNameList.setNameSpace("soc");
        dbNameList.setName("isoc_field_mappings");
        dbNameList.setUrl("");
        dbNameList.setUserName("");
        dbNameList.setPassword("");
        dbNameList.setSql("SELECT * FROM `ecs_info` WHERE STATUS=1 LIMIT 1");
        List<String> ipFieldNames = new ArrayList<>();
        ipFieldNames.add("public_ips");
        ipFieldNames.add("inner_ips");
        ipFieldNames.add("eip");
        ipFieldNames.add("private_ips");
        dbNameList.init();
        return dbNameList;
    }

    @Test
    public void testNameListAllRow() {
        AbstractDim nameList = createMapping();
        JSONObject msg = new JSONObject();
        msg.put("levelFile", "aegis-vul_record:level");
        msg.put("levelValue", "high");
        msg.put("now", "2019-07-18 17:33:29");
        long start = System.currentTimeMillis();

    }

    private AbstractDim createMapping() {
        DBDim dbNameList = new DBDim();
        dbNameList.setNameSpace("soc");
        dbNameList.setName("isoc_field_mappings");
        dbNameList.setUrl("");
        dbNameList.setUserName("");
        dbNameList.setPassword("");
        dbNameList.setSql("select * from ads_yunsec_ti_url_all_df limit 100000");
        dbNameList.init();
        return dbNameList;
    }

}
