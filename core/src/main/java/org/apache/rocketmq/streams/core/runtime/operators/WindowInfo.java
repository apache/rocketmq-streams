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


public class WindowInfo {
    private WindowType windowType;

    private Time windowSize;//窗口大小

    private Time windowSlide;//滑动大小

    private Time sessionTimeout;


    public WindowType getWindowType() {
        return windowType;
    }

    public void setWindowType(WindowType windowType) {
        this.windowType = windowType;
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

    public enum WindowType {
        SLIDING_WINDOW,
        TUMBLING_WINDOW,
        SESSION_WINDOW
    }
}
