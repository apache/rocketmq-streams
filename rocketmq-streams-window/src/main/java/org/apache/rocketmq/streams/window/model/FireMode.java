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
package org.apache.rocketmq.streams.window.model;

/**
 * the mode for supporting more scene
 *
 * @author arthur
 */

public enum FireMode {

    /**
     * 常规触发（最大延迟时间<=窗口大小）
     */
    NORMAL(0),

    /**
     * 分段触发（最大延迟时间>窗口大小）
     */
    PARTITIONED(1),

    /**
     * 增量触发（最大延迟时间>窗口大小）
     */
    ACCUMULATED(2);

    int value;

    private FireMode(int num) {
        this.value = num;
    }

    public static FireMode valueOf(int theValue) {
        switch (theValue) {
            case 0:
                return NORMAL;
            case 1:
                return PARTITIONED;
            case 2:
                return ACCUMULATED;
            default:
                return null;
        }
    }

}
