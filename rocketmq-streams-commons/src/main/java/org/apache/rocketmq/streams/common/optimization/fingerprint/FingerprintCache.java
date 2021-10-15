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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * cache log finger
 * support mutil sence shared
 * can set cache size by sdk or property file
 */
public class FingerprintCache {
    protected static FingerprintCache fingerprintCache;
    protected static int CACHE_SIZE=3000000;//default cache size，support 3000000 log size

    //key: namespace  value:FingerprintMetric
    protected Map<String,FingerprintMetric> metricMap=new HashMap<>();

    protected BitSetCache bitSetCache;
    protected int cacheSize;
    private FingerprintCache(int cacheSize){
        this.cacheSize=cacheSize;
        this.bitSetCache=new BitSetCache(this.cacheSize);
    }

    public void addLogFingerprint(String namespace,String msgKey, BitSetCache.BitSet bitSet){
        if(msgKey==null){
            return;
        }
        if(bitSetCache!=null){
            FingerprintMetric fingerprintMetric=getOrCreateMetric(namespace);
            fingerprintMetric.addCaceSize();
            this.bitSetCache.put(namespace+"->"+msgKey,bitSet);
        }
    }



    public BitSetCache.BitSet getLogFingerprint(String namespace,String msgKey){
        if(msgKey==null){
            return null;
        }
        BitSetCache.BitSet bitSet= this.bitSetCache.get(namespace+"->"+msgKey);
        FingerprintMetric fingerprintMetric=getOrCreateMetric(namespace);
        fingerprintMetric.addMetric(bitSet!=null);
        return bitSet;
    }


    public void addLogFingerprint(String namespace,IMessage message, BitSetCache.BitSet bitSet, List<String> logFingerprintFieldNames){
        String msgKey=creatFingerpringKey(message,logFingerprintFieldNames);
        addLogFingerprint(namespace,msgKey,bitSet);
    }
    public BitSetCache.BitSet getLogFingerprint(String namespace,IMessage message, List<String>  logFingerprintFieldNames){
        String msgKey=creatFingerpringKey(message,logFingerprintFieldNames);
        return getLogFingerprint(namespace,msgKey);
    }

    public FingerprintMetric getOrCreateMetric(String namespace) {
        FingerprintMetric fingerprintMetric=metricMap.get(namespace);
        if(fingerprintMetric==null){
            synchronized (this){
                fingerprintMetric=metricMap.get(namespace);
                if(fingerprintMetric==null){
                    fingerprintMetric=new FingerprintMetric(namespace);
                    metricMap.put(namespace,fingerprintMetric);
                }
            }

        }
        return fingerprintMetric;
    }

    public static FingerprintCache getInstance(){
        if(fingerprintCache==null){
            synchronized (FingerprintCache.class){
                if(fingerprintCache==null){
                    fingerprintCache=new FingerprintCache(CACHE_SIZE);
                }
            }
        }
        return fingerprintCache;
    }



    static {
        String sizeValue= ComponentCreator.getProperties().getProperty("fingerprint.cache.size");
        if(StringUtil.isNotEmpty(sizeValue)){
            CACHE_SIZE=Integer.valueOf(sizeValue);
        }
    }


    /**
     * 创建代表日志指纹的字符串
     *
     * @param message
     * @return
     */
    public static String creatFingerpringKey(IMessage message, List<String> logFingerprintFieldNames) {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (String value : logFingerprintFieldNames) {
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
