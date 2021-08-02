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
package org.apache.rocketmq.streams.window.offset;

import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.rocketmq.streams.common.model.Entity;

/**
 * save windowintance max offset
 */
public class WindowMaxValue extends Entity {
    public static long MAX_VALUE_BASE_VALUE=100000000;
    protected String msgKey;
    protected AtomicLong maxValue=new AtomicLong(MAX_VALUE_BASE_VALUE);

    protected AtomicLong maxEventTime=new AtomicLong();//只有window需要

    public WindowMaxValue(){
        this.gmtModified=new Date();
        this.gmtCreate=new Date();
    }

    public Long getMaxEventTime() {
        return maxEventTime.get();
    }

    public void setMaxEventTime(Long maxEventTime) {
        if(maxEventTime==null){
            return;
        }
        this.maxEventTime.set( maxEventTime);
    }

    public String getMsgKey() {
        return msgKey;
    }

    public void setMsgKey(String msgKey) {
        this.msgKey = msgKey;
    }

    public Long getMaxValue() {
        return maxValue.get();
    }

    public void setMaxValue(Long maxValue) {
        this.maxValue.set(maxValue);
    }

    public long comareAndSet(Long eventTime){
        if(eventTime==null){
            return maxEventTime.get();
        }
        long old=maxEventTime.get();
        if(old>=eventTime){
            return old;
        }
        boolean updateSuccess=false;
        while (!updateSuccess){
            old=maxEventTime.get();
            if(eventTime>old){
                updateSuccess= maxEventTime.compareAndSet(old,eventTime);
            } else {
                break;
            }
        }
        return maxEventTime.get();
    }

    public long incrementAndGetMaxOffset(){
        return maxValue.incrementAndGet();
    }
}
