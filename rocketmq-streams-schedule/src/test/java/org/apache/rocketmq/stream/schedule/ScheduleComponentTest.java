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
package org.apache.rocketmq.stream.schedule;

import org.apache.rocketmq.streams.common.interfaces.IScheduleExecutor;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.schedule.ScheduleComponent;
import org.junit.Test;

public class ScheduleComponentTest {
    private ScheduleComponent scheduleComponent = ScheduleComponent.getInstance();

    @Test
    public void testSchedule() throws InterruptedException {

        scheduleComponent.getService().startSchedule(create(), "2/2 * * * * ?", true);

        while (true) {
            Thread.sleep(1000);
        }
    }

    protected IScheduleExecutor create() {
        IScheduleExecutor channelExecutor = new IScheduleExecutor() {
            @Override
            public void doExecute() throws InterruptedException {
                System.out.println(DateUtil.getCurrentTimeString());
            }

            @Override
            public String getName() {
                return "name";
            }

            @Override
            public String getNameSpace() {
                return "namespace";
            }

            @Override
            public String getType() {
                return "type";
            }

        };
        return channelExecutor;
    }
}
