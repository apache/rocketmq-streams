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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;

public class URLIntelligenceCache extends AbstractIntelligenceCache implements IAfterConfiguableRefreshListerner {

    private static final Log LOG = LogFactory.getLog(URLIntelligenceCache.class);

    protected transient String keyName = "url";

    @Override
    protected String getSQL() {
        return "SELECT id, url, `is_malicious_source`  FROM `ads_yunsec_ti_url_all_df` where curdate() < date_add(modify_time, interval expire_time day) ";
    }

    @Override
    public Map<String, Object> getRow(String ip) {
        Integer value = intValueKV.get(ip);
        if (value == null) {
            return null;
        }
        Map<String, Object> row = new HashMap<>();

        row.put("is_malicious_source", getNumBitValue(value, 0));
        return row;
    }

    @Override
    public String getKeyName() {
        return this.keyName;
    }

    @Override
    public String getTableName() {
        return "ads_yunsec_ti_url_all_df";
    }

    @Override
    protected void doProccRows(IntValueKV intValueKV, List<Map<String, Object>> rows, int index) {
        for (Map<String, Object> row : rows) {
            String ip = (String)row.get(keyName);
            if (ip == null) {
                LOG.warn("load Intelligence exception ,the ip is null");
                continue;
            }
            List<String> values = new ArrayList<>();
            values.add((String)row.get("is_malicious_source"));
            int value = createInt(values);
            synchronized (this) {
                intValueKV.put(ip, value);
            }

        }
    }

}
