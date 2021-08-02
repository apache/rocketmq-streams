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
package org.apache.rocketmq.streams.common.optimization;

import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;

public class SQLLogFingerprintFilter extends LogFingerprintFilter {

    protected static SQLLogFingerprintFilter sqlLogFingerprintFilter = new SQLLogFingerprintFilter();

    public static SQLLogFingerprintFilter getInstance() {
        return sqlLogFingerprintFilter;
    }

    public <T extends IMessage> String createMessageKey(T message, String logFingerFieldNames, String logFingerFilterStageName) {
        String key = super.createMessageKey(message, logFingerFieldNames);
        return MapKeyUtil.createKeyBySign(".", logFingerFilterStageName, key);
    }

    /**
     * 增加缓存，对于某个规则未触发的日志进行缓存，可以快速过滤掉这个日志对这个规则的判读
     *
     * @param msgKey     资产指纹
     * @param filterName piplinenamespace.piplinename.filterlableorindex
     */
    public void addNoFireMessage(String msgKey, String filterName) {
        if (getRowCount() > MAX_COUNT) {
            synchronized (this) {
                if (getRowCount() > MAX_COUNT) {
                    noFireMessages = new IntValueKV(MAX_COUNT);
                }
            }
        }
        noFireMessages.put(msgKey, 1);
    }

}
