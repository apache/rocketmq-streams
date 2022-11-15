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
package org.apache.rocketmq.streams.core.runtime.operators;

import org.apache.rocketmq.streams.core.common.Constant;

public class WindowKey {
    private String operatorName;

    private Long windowStart;

    private Long windowEnd;

    private String key2String;

    public WindowKey(String operatorName, String key2String, Long windowEnd, Long windowStart) {
        this.operatorName = operatorName;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.key2String = key2String;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public Long getWindowStart() {
        return windowStart;
    }

    public void setWindowStart(Long windowStart) {
        this.windowStart = windowStart;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public String getKey2String() {
        return key2String;
    }

    public void setKey2String(String key2String) {
        this.key2String = key2String;
    }

    public String getSearchKeyPrefixNameAndKey() {
        StringBuilder builder = new StringBuilder();
        builder.append(operatorName)
                .append(Constant.SPLIT)
                .append(key2String);

        return builder.toString();
    }

    public String getSearchKeyPrefixName() {
        return this.operatorName;
    }

    public String getKeyAndWindow() {
        StringBuilder builder = new StringBuilder();
        builder.append(windowStart)
                .append(Constant.SPLIT)
                .append(windowEnd)
                .append(Constant.SPLIT)
                .append(key2String);

        return builder.toString();
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(operatorName)
                .append(Constant.SPLIT)
                .append(key2String)
                .append(Constant.SPLIT)
                .append(windowEnd)
                .append(Constant.SPLIT)
                .append(windowStart)
        ;

        return builder.toString();
    }
}
