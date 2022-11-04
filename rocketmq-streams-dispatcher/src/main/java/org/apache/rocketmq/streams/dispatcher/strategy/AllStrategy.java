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
package org.apache.rocketmq.streams.dispatcher.strategy;

import com.google.common.collect.Lists;
import java.util.List;
import org.apache.rocketmq.streams.dispatcher.IStrategy;
import org.apache.rocketmq.streams.dispatcher.entity.DispatcherMapper;

public class AllStrategy implements IStrategy {
    @Override public DispatcherMapper dispatch(List<String> tasks, List<String> instances) throws Exception {
        DispatcherMapper dispatcherMapper = new DispatcherMapper();
        for (String instance : instances) {
            for (String task : tasks) {
                dispatcherMapper.putTask(instance, task);
            }
        }
        return dispatcherMapper;
    }

    public static void main(String[] args) {
        List<String> tasks = Lists.newArrayList("t1", "t2", "t3");
        List<String> instances = Lists.newArrayList("ins1", "ins2", "ins3", "ins4", "ins5");
        AllStrategy allStrategy = new AllStrategy();

        try {
            DispatcherMapper dispatcherMapper = allStrategy.dispatch(tasks, instances);
            for (String instance : instances) {
                System.out.println(instance + ":" + dispatcherMapper.getTasks(instance));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }
}
