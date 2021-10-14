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
package org.apache.rocketmq.streams.client.transform.window;

import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.SessionOperator;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;

/**
 * 保存创建window的信息 主要是窗口类型，窗口大小
 */
public class WindowInfo {
    public static int HOPPING_WINDOW = 1;//滑动窗口
    public static int TUMBLING_WINDOW = 2;//滚动窗口
    public static int SESSION_WINDOW = 3;
    protected int type;//window类型 hopping，Tumbling
    protected Time windowSize;//窗口大小
    protected Time windowSlide;//滑动大小
    /**
     * 会话窗口的超时时间
     */
    protected Time sessionTimeout;

    protected String timeField;

    /**
     * 创建窗口
     *
     * @return
     */
    public AbstractWindow createWindow() {
        AbstractWindow window = null;
        if (type == HOPPING_WINDOW) {
            window = new WindowOperator();
            window.setTimeUnitAdjust(1);
            window.setSizeInterval(windowSize.getValue());
            window.setSlideInterval(windowSlide.getValue());
        } else if (type == TUMBLING_WINDOW) {
            window = new WindowOperator();
            window.setTimeUnitAdjust(1);
            window.setSizeInterval(windowSize.getValue());
        } else if (type == SESSION_WINDOW) {
            window = new SessionOperator(sessionTimeout.getValue());
        } else {
            throw new RuntimeException("can not support the type ,expect 1，2，3。actual is " + type);
        }
        window.setTimeFieldName(timeField);
        return window;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

    public Time getWindowSize() {
        return windowSize;
    }

    public void setWindowSize(Time windowSize) {
        this.windowSize = windowSize;
    }

    public Time getWindowSlide() {
        return windowSlide;
    }

    public void setWindowSlide(Time windowSlide) {
        this.windowSlide = windowSlide;
    }

    public Time getSessionTimeout() {
        return sessionTimeout;
    }

    public void setSessionTimeout(Time sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    public String getTimeField() {
        return timeField;
    }

    public void setTimeField(String timeField) {
        this.timeField = timeField;
    }
}
