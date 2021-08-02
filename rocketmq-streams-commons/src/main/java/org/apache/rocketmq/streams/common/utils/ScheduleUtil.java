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
package org.apache.rocketmq.streams.common.utils;

import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.interfaces.IScheduleExecutor;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;

public class ScheduleUtil {

    public static IScheduleExecutor convert(IStreamOperator receiver) {
        if (!(receiver instanceof IConfigurable)) {
            throw new RuntimeException("can not convert to IScheduleExecutor, need function Iconfiguable interface");
        }
        IConfigurable configurable = (IConfigurable)receiver;
        return new IScheduleExecutor() {
            @Override
            public void doExecute() throws InterruptedException {
                receiver.doMessage(null, null);
            }

            @Override
            public String getConfigureName() {
                return configurable.getConfigureName();
            }

            @Override
            public String getNameSpace() {
                return configurable.getNameSpace();
            }

            @Override
            public String getType() {
                return configurable.getType();
            }
        };
    }
}
