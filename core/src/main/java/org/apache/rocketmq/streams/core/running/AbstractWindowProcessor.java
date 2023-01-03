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
package org.apache.rocketmq.streams.core.running;


import org.apache.rocketmq.streams.core.window.Window;
import org.apache.rocketmq.streams.core.window.WindowInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractWindowProcessor<V> extends AbstractProcessor<V> {
    private static final Logger logger = LoggerFactory.getLogger(AbstractWindowProcessor.class.getName());


    protected List<Window> calculateWindow(WindowInfo windowInfo, long valueTime) {
        long sizeInterval = windowInfo.getWindowSize().toMillSecond();
        long slideInterval = windowInfo.getWindowSlide().toMillSecond();

        List<Window> result = new ArrayList<>((int) (sizeInterval / slideInterval));
        long lastStart = valueTime - (valueTime + slideInterval) % slideInterval;

        for (long start = lastStart; start > valueTime - sizeInterval; start -= slideInterval) {
            long end = start + sizeInterval;
            Window window = new Window(start, end);
            result.add(window);
        }
        return result;
    }

}
