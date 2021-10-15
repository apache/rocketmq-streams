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

import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.SQLLogFingerprintFilter;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * filter message by log fingerprint
 * create log fingerprint in filter stage, then  regist log fingerprin to source stage
 * execute filter in source stage, if match filte directly
 */
public class PreFingerprint {
    protected transient String logFingerFieldNames;//如果有日志指纹，这里存储日志指纹的字段，启动时，通过属性文件加载
    protected transient String filterStageIdentification;//唯一标识一个filter

    protected transient String sourceStageLable;//execute logfinger filter's stage, may be owner mutil branch stage or pipeline source
    protected transient String nextStageLable;//the source stage's next stage lable
    protected transient FingerprintCache fingerprintCache;
    public PreFingerprint(String logFingerFieldNames,String filterStageIdentification,String sourceStageLable,String nextStageLable){
        this.logFingerFieldNames=logFingerFieldNames;
        this.filterStageIdentification=filterStageIdentification;
        this.sourceStageLable=sourceStageLable;
        this.nextStageLable=nextStageLable;
        this.fingerprintCache= FingerprintCache.getInstance();

    }


    /**
     * 通过日志指纹过滤，如果有过滤日志指纹字段，做过滤判断
     *
     * @param message
     * @return
     */
    public boolean filterByLogFingerprint(IMessage message) {
        if (logFingerFieldNames != null) {
            String msgKey = FingerprintCache.creatFingerpringKey(message,filterStageIdentification,logFingerFieldNames);
            if (msgKey != null) {
                BitSetCache.BitSet bitSet = fingerprintCache.getLogFingerprint(filterStageIdentification,msgKey);
                if (bitSet != null && bitSet.get(0)) {
                    return true;
                } else {
                    message.getHeader().setLogFingerprintValue(msgKey);
                }
            }
        }
        return false;
    }


    /**
     * 设置过滤指纹
     *
     * @param message
     */
    public void addLogFingerprintToSource(IMessage message) {

        String msgKey = message.getHeader().getLogFingerprintValue();
        if(msgKey!=null){
            BitSetCache.BitSet bitSet =new BitSetCache.BitSet(1);
            bitSet.set(0);
            fingerprintCache.addLogFingerprint(filterStageIdentification,msgKey,bitSet);
        }
    }

    public static void main(String[] args) {
        BitSetCache.BitSet bitSet =new BitSetCache.BitSet(1);
        bitSet.set(0);
        System.out.println(bitSet.get(0));
    }


    protected SQLLogFingerprintFilter createLogFingerprintFilter() {
        return SQLLogFingerprintFilter.getInstance();
    }

    public String getSourceStageLable() {
        return sourceStageLable;
    }

    public String getNextStageLable() {
        return nextStageLable;
    }
}
