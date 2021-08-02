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
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/***
 * 存储日志指纹，对于相同的日志指纹的规则，可以不用计算直接过滤掉
 * 主要用在数据源重复率比较高的场景
 */
public class LogFingerprintFilter {
    protected static final int MAX_COUNT = 3000000;
    protected int maxCount = MAX_COUNT;
    protected IntValueKV noFireMessages = new IntValueKV(MAX_COUNT);

    public LogFingerprintFilter() {}

    public LogFingerprintFilter(int maxCount) {
        this.maxCount = maxCount;
    }

    /**
     * 增加缓存，对于某个规则未触发的日志进行缓存，可以快速过滤掉这个日志对这个规则的判读
     *
     * @param msgKey       资产指纹
     * @param piplineIndex 规则索引
     */
    public void addNoFireMessage(String msgKey, int piplineIndex) {
        Integer value = noFireMessages.get(msgKey);
        if (value == null) {
            value = 0;
        }
        if (canFilter(msgKey, piplineIndex)) {
            return;
        }
        value = NumberUtils.setNumFromBitMapInt(value, piplineIndex);
        /**
         * 因为日志会不停的增加，当超过最大值时，丢弃掉所有缓存，重新累计
         */
        if (getRowCount() > MAX_COUNT) {
            synchronized (this) {
                if (getRowCount() > MAX_COUNT) {
                    noFireMessages = new IntValueKV(MAX_COUNT);
                }
            }
        }
        noFireMessages.put(msgKey, value);
    }

    /**
     * 获取二进制map的int形式
     *
     * @param msgkey 资产指纹
     * @return
     */
    public Integer getFilterValue(String msgkey) {
        Integer value = noFireMessages.get(msgkey);
        if (value == null) {
            value = 0;
        }
        return value;
    }

    /**
     * @param filterValue  二进制位map，根据索引判读某一位是0还1，如果是1代表能过滤
     * @param piplineIndex 读取int 对应的二进制数组的哪一位
     * @return
     */
    public boolean canFilter(Integer filterValue, int piplineIndex) {
        if (filterValue == null) {
            return false;
        }
        return NumberUtils.getNumFromBitMapInt(filterValue, piplineIndex);
    }

    /**
     * 是否某个规则能过滤掉这个日志，根据日志指纹，判读有无做缓存，如果缓存，说明曾经未做过触发，直接过滤
     *
     * @param msgKey 日志指纹，多个核心日志字段拼接而成
     * @return
     */
    public boolean canFilter(String msgKey, int piplineIndex) {
        Integer value = noFireMessages.get(msgKey);
        if (value == null) {
            return false;
        }
        return NumberUtils.getNumFromBitMapInt(value, piplineIndex);
    }

    /**
     * 获取缓存的行数
     *
     * @return
     */
    public int getRowCount() {
        return noFireMessages.getSize();
    }

    /**
     * 获取占用内存大小
     *
     * @return
     */
    public int getByteSize() {
        return noFireMessages.calMemory();
    }

    /**
     * 创建代表日志指纹的字符串
     *
     * @param message
     * @return
     */
    public String createMessageKey(IMessage message, String filterMsgFieldNames) {
        String[] values = filterMsgFieldNames.split(",");
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (String value : values) {
            String msgValue = message.getMessageBody().getString(value);
            if (StringUtil.isEmpty(msgValue)) {
                msgValue = "<NULL>";
            }

            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(";");
            }
            sb.append(msgValue);
        }
        return sb.toString();

    }

}
