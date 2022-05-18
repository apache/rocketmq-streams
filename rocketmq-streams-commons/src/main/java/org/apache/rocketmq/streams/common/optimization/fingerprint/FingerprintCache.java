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
package org.apache.rocketmq.streams.common.optimization.fingerprint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * cache log finger
 * support mutil sence shared
 * can set cache size by sdk or property file
 */
public class FingerprintCache {
    protected static FingerprintCache fingerprintCache;
    protected static int CACHE_SIZE = 5000000;//default cache size，support 3000000 log size
    public static String FIELD_VALUE_SPLIT_SIGN=";;;;;";
    //key: namespace  value:FingerprintMetric
    protected Map<String, FingerprintMetric> metricMap = new HashMap<>();

    protected BitSetCache bitSetCache;
    protected int cacheSize;
    protected int reHashCount = 0;
    protected FingerprintMetric rootFingerprintMetric = new FingerprintMetric("root");
    protected Long firstUpdateTime;
    protected double minHitCacheRate = 0.4;

    public FingerprintCache(int cacheSize) {
        this.cacheSize = cacheSize;
        this.bitSetCache = new BitSetCache(this.cacheSize);
    }

    public void addLogFingerprint(String namespace, String msgKey, BitSetCache.BitSet bitSet) {
        if (msgKey == null) {
            return;
        }

        if (bitSetCache != null) {
            FingerprintMetric fingerprintMetric = getOrCreateMetric(namespace);
            fingerprintMetric.addCaceSize();
            this.rootFingerprintMetric.addCaceSize();
            if (fingerprintMetric.isCloseFingerprint()) {
                return;
            }
            if (this.bitSetCache.size() > CACHE_SIZE) {
                synchronized (this) {
                    if (this.bitSetCache.size() > CACHE_SIZE) {

                        executeCloseStrategy();
                        for (FingerprintMetric metric : this.metricMap.values()) {
                            metric.clear();
                        }
                        this.bitSetCache = new BitSetCache(this.cacheSize);
                        reHashCount++;
                        this.rootFingerprintMetric.clear();
                        firstUpdateTime = System.currentTimeMillis();
                    }
                }
            }
            this.bitSetCache.put(namespace + "->" + msgKey, bitSet);
        }
    }

    protected void executeCloseStrategy() {
        this.rootFingerprintMetric.print();
        if (System.currentTimeMillis() - firstUpdateTime > 1000 * 60 * 60 * 4) {
            return;
        }
        if (metricMap.size() == 1) {
            return;
        }
        List<FingerprintMetric> fingerprintMetricList = new ArrayList<>(metricMap.values());
        for (int i = 0; i < fingerprintMetricList.size() - 1; i++) {
            FingerprintMetric fingerprintMetric = fingerprintMetricList.get(i);
            if (!fingerprintMetric.isCloseFingerprint() && fingerprintMetric.getHitCacheRate() < this.minHitCacheRate) {
                fingerprintMetric.setCloseFingerprint(true);
                System.out.println("close fingerprint " + PrintUtil.LINE);
                fingerprintMetric.print();
            }
        }

    }

    public BitSetCache.BitSet getLogFingerprint(String namespace, String msgKey) {
        if (msgKey == null) {
            return null;
        }
        FingerprintMetric fingerprintMetric = getOrCreateMetric(namespace);
        if (fingerprintMetric.isCloseFingerprint()) {
            return null;
        }
        if (firstUpdateTime == null) {
            firstUpdateTime = System.currentTimeMillis();
        }
        BitSetCache.BitSet bitSet = this.bitSetCache.get(namespace + "->" + msgKey);
        this.rootFingerprintMetric.addMetric(bitSet != null);
        fingerprintMetric.addMetric(bitSet != null);
        return bitSet;
    }

    public void addLogFingerprint(String namespace, IMessage message, BitSetCache.BitSet bitSet,
        String logFingerprintFieldNames) {
        String msgKey = creatFingerpringKey(message, namespace, logFingerprintFieldNames);
        addLogFingerprint(namespace, msgKey, bitSet);
    }

    public BitSetCache.BitSet getLogFingerprint(String namespace, IMessage message, String logFingerprintFieldNames) {
        String msgKey = creatFingerpringKey(message, namespace, logFingerprintFieldNames);
        return getLogFingerprint(namespace, msgKey);
    }

    public FingerprintMetric getOrCreateMetric(String namespace) {
        FingerprintMetric fingerprintMetric = metricMap.get(namespace);
        if (fingerprintMetric == null) {
            synchronized (this) {
                fingerprintMetric = metricMap.get(namespace);
                if (fingerprintMetric == null) {
                    fingerprintMetric = new FingerprintMetric(namespace);
                    metricMap.put(namespace, fingerprintMetric);
                }
            }

        }
        return fingerprintMetric;
    }

    public static FingerprintCache getInstance() {
        if (fingerprintCache == null) {
            synchronized (FingerprintCache.class) {
                if (fingerprintCache == null) {
                    fingerprintCache = new FingerprintCache(CACHE_SIZE);
                }
            }
        }
        return fingerprintCache;
    }

    static {
        String sizeValue = ComponentCreator.getProperties().getProperty("fingerprint.cache.size");
        if (StringUtil.isNotEmpty(sizeValue)) {
            CACHE_SIZE = Integer.valueOf(sizeValue);
        }
    }

    protected static Map<String, List<String>> logFingerprintFieldNameListMap = new HashMap<>();

    /**
     * 创建代表日志指纹的字符串
     *
     * @param message
     * @return
     */
    public static String creatFingerpringKey(IMessage message, String namespace, String logFingerprintFieldNames) {
        String key = MapKeyUtil.createKey(namespace, logFingerprintFieldNames);
        List<String> logFingerprintFieldNameList = logFingerprintFieldNameListMap.get(key);
        if (logFingerprintFieldNameList == null) {
            synchronized (FingerprintCache.class) {
                logFingerprintFieldNameList = logFingerprintFieldNameListMap.get(key);
                if (logFingerprintFieldNameList == null) {

                    if (logFingerprintFieldNames != null) {
                        List<String> list = new ArrayList<>();
                        for (String name : logFingerprintFieldNames.split(",")) {
                            list.add(name);
                        }
                        logFingerprintFieldNameList = list;
                    }
                }
            }
        }
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (String value : logFingerprintFieldNameList) {
            String msgValue = message.getMessageBody().getString(value);
            if (StringUtil.isEmpty(msgValue)) {
                msgValue = "<NULL>";
            }

            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(FIELD_VALUE_SPLIT_SIGN);
            }
            sb.append(msgValue);
        }
        return sb.toString();

    }

    public double getMinHitCacheRate() {
        return minHitCacheRate;
    }

    public void setMinHitCacheRate(double minHitCacheRate) {
        this.minHitCacheRate = minHitCacheRate;
    }
}
