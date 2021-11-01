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

import com.alibaba.fastjson.JSONObject;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;

public class FingerprintMetric {
    protected String namespace;
    protected AtomicLong visitCount=new AtomicLong(0);
    protected AtomicLong hitCacheCount=new AtomicLong(0);
    protected AtomicLong cacheSize=new AtomicLong(0);
    protected boolean isCloseFingerprint=false;

    public FingerprintMetric(String namespace){
        this.namespace=namespace;
    }

    public void addMetric(boolean isHitCache){
        visitCount.incrementAndGet();
        if(isHitCache){
            hitCacheCount.incrementAndGet();
        }
    }


    public void addCaceSize(){
        cacheSize.incrementAndGet();
    }

    public Long getVisitCount(){
        return this.visitCount.get();
    }

    public Long getHitCacheCount(){
        return this.hitCacheCount.get();
    }



    public Long getCacheSize(){
        return this.cacheSize.get();
    }

    public double getHitCacheRate(){
        double visitCount=getVisitCount();
        if(visitCount==0){
            visitCount=1;
        }
        double hitCacheCount=getHitCacheCount();
        return hitCacheCount/visitCount;
    }

    public boolean isCloseFingerprint() {
        return isCloseFingerprint;
    }

    public void setCloseFingerprint(boolean closeFingerprint) {
        isCloseFingerprint = closeFingerprint;
    }

    public void clear() {
        visitCount.set(0);
        hitCacheCount.set(0);
        cacheSize.set(0);
    }

    public void print() {
        JSONObject msg=new JSONObject();
        msg.put("visitCount",visitCount.get());
        msg.put("hitCacheCount",hitCacheCount.get());
        msg.put("cacheSize",cacheSize.get());
        msg.put("hitCacheRate",getHitCacheRate());
        msg.put("namespace",namespace);
        msg.put("isClosed",isCloseFingerprint);
        System.out.println(JsonableUtil.formatJson(msg));
    }

    public String getNamespace() {
        return namespace;
    }
}
