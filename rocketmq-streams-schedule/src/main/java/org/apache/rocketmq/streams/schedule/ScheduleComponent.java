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
package org.apache.rocketmq.streams.schedule;

import org.apache.rocketmq.streams.schedule.service.IScheduleService;
import org.apache.rocketmq.streams.schedule.service.impl.ScheduleServiceImpl;
import org.apache.rocketmq.streams.common.component.AbstractComponent;
import org.apache.rocketmq.streams.common.component.ComponentCreator;

import java.util.Properties;

public class ScheduleComponent extends AbstractComponent<IScheduleService> {
    private static ScheduleComponent scheduleComponent;
    protected ScheduleServiceImpl scheduleService = new ScheduleServiceImpl();

    @Override
    public boolean stop() {
        scheduleService.stop();
        return true;
    }

    public static ScheduleComponent getInstance() {
        if (scheduleComponent != null) {
            return scheduleComponent;
        }
        synchronized (ScheduleComponent.class) {
            if (scheduleComponent != null) {
                return scheduleComponent;
            }
            ScheduleComponent tmp = ComponentCreator.getComponent(null, ScheduleComponent.class);
            scheduleComponent = tmp;
        }
        return scheduleComponent;
    }

    @Override
    public IScheduleService getService() {
        return scheduleService;
    }

    @Override
    protected boolean startComponent(String name) {
        scheduleService.start();
        return true;
    }

    @Override
    protected boolean initProperties(Properties properties) {
        return true;
    }
}
