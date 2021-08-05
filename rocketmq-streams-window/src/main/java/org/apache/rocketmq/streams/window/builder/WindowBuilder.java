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
package org.apache.rocketmq.streams.window.builder;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.OverWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.apache.rocketmq.streams.window.operator.join.JoinWindow;

public class WindowBuilder {
    /**
     * 默认窗口大小
     */
    public static final int DEFAULT_WINDOW_INTERVAL_SIZE_MINUTE = 5;
    private static boolean TEST_MODE = false;

    public static WindowOperator createWindow() {
        if (!TEST_MODE) {
            return new WindowOperator();
        } else {
            return new WindowOperator();
        }
    }

    public static void openTestModel() {
        TEST_MODE = true;
    }

    public static void closeTestModel() {
        TEST_MODE = false;
    }

    /**
     * 创建join的窗口对象，join的窗口大小可以配置文件配置，如果未配置用写死的默认值
     *
     * @return
     */
    public static JoinWindow createDefaultJoinWindow() {
        JoinWindow joinWindow = new JoinWindow();
        if (TEST_MODE) {
            joinWindow = new JoinWindow();
        }
        joinWindow.setSizeInterval(getIntValue(ConfigureFileKey.DIPPER_WINDOW_JOIN_DEFAULT_ITERVA_SIZE, 5));//默认5分钟一个窗口
        joinWindow.setRetainWindowCount(getIntValue(ConfigureFileKey.DIPPER_WINDOW_JOIN_RETAIN_WINDOW_COUNT, 6));//join的时间窗口是20分钟
        joinWindow.setWindowType(AbstractWindow.TUMBLE_WINDOW);
        //  joinWindow.setFireDelaySecond(getIntValue(ConfigureFileKey.DIPPER_WINDOW_DEFAULT_FIRE_DELAY_SECOND,5));//延迟5分钟触发
        joinWindow.setTimeFieldName("");
        joinWindow.setSlideInterval(getIntValue(ConfigureFileKey.DIPPER_WINDOW_JOIN_DEFAULT_ITERVA_SIZE, 5));
        joinWindow.setWaterMarkMinute(0);
        joinWindow.setWindowType(AbstractWindow.TUMBLE_WINDOW);
        return joinWindow;
    }

    public static OverWindow createOvertWindow(String groupBy, String rowNumName) {
        OverWindow overWindow = new OverWindow();
        overWindow.setGroupByFieldName(groupBy);
        overWindow.setRowNumerName(rowNumName);
        overWindow.setTimeFieldName("");
        overWindow.setSizeInterval(getIntValue(ConfigureFileKey.DIPPER_WINDOW_OVER_DEFAULT_ITERVA_SIZE, 60));
        overWindow.setSlideInterval(overWindow.getSizeInterval());
        return overWindow;
    }

    /**
     * 获取配置配置文件的值，如果配置文件为配置，则用默认值
     *
     * @param propertyKey
     * @param defalutValue
     * @return
     */
    public static int getIntValue(String propertyKey, int defalutValue) {
        String value = ComponentCreator.getProperties().getProperty(propertyKey);
        if (StringUtil.isNotEmpty(value)) {
            return Integer.valueOf(value);
        }
        return defalutValue;
    }
}
