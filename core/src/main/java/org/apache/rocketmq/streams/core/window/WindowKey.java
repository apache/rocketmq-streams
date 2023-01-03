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
package org.apache.rocketmq.streams.core.window;

import org.apache.rocketmq.streams.core.util.Utils;

import java.nio.charset.StandardCharsets;

public class WindowKey {
    private static final String SPLIT = "&&";

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


    public String getKeyAndWindow() {
        StringBuilder builder = new StringBuilder();
        builder.append(windowStart)
                .append(WindowKey.SPLIT)
                .append(windowEnd)
                .append(WindowKey.SPLIT)
                .append(key2String);

        return builder.toString();
    }

    public static WindowKey byte2WindowKey(byte[] source) {
        String str = new String(source, StandardCharsets.UTF_8);
        String[] split = Utils.split(str, WindowKey.SPLIT);
        return new WindowKey(split[0], split[1],  Long.parseLong(split[2]), Long.parseLong(split[3]));
    }


    public static byte[] windowKey2Byte(WindowKey windowKey) {
        if (windowKey == null) {
            return new byte[0];
        }

        return windowKey.toString().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(operatorName)
                .append(WindowKey.SPLIT)
                .append(key2String)
                .append(WindowKey.SPLIT)
                .append(windowEnd)
                .append(WindowKey.SPLIT)
                .append(windowStart)
        ;

        return builder.toString();
    }
}
