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
package org.apache.rocketmq.streams.dim.intelligence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.dboperator.IDBDriver;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IPIntelligenceCache extends AbstractIntelligenceCache implements IAfterConfigurableRefreshListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(IPIntelligenceCache.class);
    protected transient String keyName = "ip";

    @Override
    protected String getSQL() {
        return "SELECT id,ip, `is_web_attack` , `is_tor` , `is_proxy` , `is_nat` , `is_mining_pool` , `is_c2` , "
            + "`is_malicious_source` , `is_3rd` , `is_idc` , `is_malicious_login`  FROM `ads_yunsec_ti_ip_all_df` where curdate() < date_add(modify_time, interval expire_time day) ";
    }

    @Override
    public String getKeyName() {
        return this.keyName;
    }

    @Override
    public String getTableName() {
        return "ads_yunsec_ti_ip_all_df";
    }

    @Override
    public Map<String, Object> getRow(String ip) {

        Integer value = intValueKV.get(ip);
        if (value == null) {
            return null;
        }
        Map<String, Object> row = new HashMap<>();

        row.put("is_web_attack", getNumBitValue(value, 0));
        row.put("is_tor", getNumBitValue(value, 1));
        row.put("is_proxy", getNumBitValue(value, 2));
        row.put("is_nat", getNumBitValue(value, 3));
        row.put("is_mining_pool", getNumBitValue(value, 4));
        row.put("is_c2", getNumBitValue(value, 5));
        row.put("is_malicious_source", getNumBitValue(value, 6));
        row.put("is_3rd", getNumBitValue(value, 7));
        row.put("is_idc", getNumBitValue(value, 8));
        row.put("is_malicious_login", getNumBitValue(value, 9));
        return row;
    }

    @Override
    protected void doProccRows(IntValueKV intValueKV, List<Map<String, Object>> rows, int index) {
        for (Map<String, Object> row : rows) {
            String ip = (String) row.get(keyName);
            if (ip == null) {
                LOGGER.warn("load Intelligence exception ,the ip is null");
                continue;
            }
            List<String> values = new ArrayList<>();
            values.add((String) row.get("is_web_attack"));
            values.add((String) row.get("is_tor"));
            values.add((String) row.get("is_proxy"));
            values.add((String) row.get("is_nat"));
            values.add((String) row.get("is_mining_pool"));
            values.add((String) row.get("is_c2"));
            values.add((String) row.get("is_malicious_source"));
            values.add((String) row.get("is_3rd"));
            values.add((String) row.get("is_idc"));
            values.add((String) row.get("is_malicious_login"));
            int value = createInt(values);
            synchronized (this) {
                intValueKV.put(ip, value);
            }
        }
    }

    public static void main(String[] args) {
        ComponentCreator.setProperties(
            "siem.properties");
        IPIntelligenceCache ipIntelligenceCache = new IPIntelligenceCache();
        IDBDriver outputDataSource = DriverBuilder.createDriver();
        ipIntelligenceCache.startLoadData(ipIntelligenceCache.getSQL(), outputDataSource);
    }
}
