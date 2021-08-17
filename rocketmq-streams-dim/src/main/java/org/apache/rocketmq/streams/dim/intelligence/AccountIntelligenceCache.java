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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;

/**
 * table: ads_yunsec_abnormal_account
 */
public class AccountIntelligenceCache extends AbstractIntelligenceCache implements IAfterConfiguableRefreshListerner {

    private static final Log LOG = LogFactory.getLog(AccountIntelligenceCache.class);

    /**
     * 情报域名
     */
    protected transient String keyName = "account";

    @Override
    public Map<String, Object> getRow(String account) {
        Integer value = intValueKV.get(account);
        if (value == null) {
            return null;
        }
        Map<String, Object> row = new HashMap<String, Object>() {{
            put("account", account);
        }};
        return row;
    }

    @Override
    protected String getSQL() {
        return "SELECT id, `account` FROM `ads_yunsec_abnormal_account`";
    }

    @Override
    public String getKeyName() {
        return this.keyName;
    }

    @Override
    public String getTableName() {
        return "ads_yunsec_abnormal_account";
    }

    @Override
    protected void doProccRows(IntValueKV intValueKV, List<Map<String, Object>> rows, int index) {
        rows.forEach(row -> {
            String account = (String)row.get(keyName);
            if (account != null) {
                intValueKV.put(account, 1);
            }
        });
    }

}
