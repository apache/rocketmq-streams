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
import java.util.Collections;
import java.util.List;
import org.apache.rocketmq.streams.dispatcher.IStrategy;
import org.apache.rocketmq.streams.dispatcher.entity.DispatcherMapper;

public class AveragelyStrategy implements IStrategy {

    public static void main(String[] args) {
        List<String> tasks = Lists.newArrayList("sas_proc->0", "sas_proc->1");
        List<String> instances = Lists.newArrayList("ins1", "ins2");
        AveragelyStrategy averagelyStrategy = new AveragelyStrategy();

        try {
            DispatcherMapper dispatcherMapper = averagelyStrategy.dispatch(tasks, instances);
            System.out.println(dispatcherMapper.getTasks("ins1"));
            System.out.println(dispatcherMapper.getTasks("ins2"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override public DispatcherMapper dispatch(List<String> tasks, List<String> instances) throws Exception {
        DispatcherMapper dispatcherMapper = new DispatcherMapper();
        Collections.sort(tasks);
        Collections.sort(instances);
        for (String instance : instances) {
            List<String> task4Instance = average(instance, tasks, instances);
            for (String task : task4Instance) {
                dispatcherMapper.putTask(instance, task);
            }
        }
        return dispatcherMapper;
    }

    private List<String> average(String instanceId, List<String> tasks, List<String> instances) throws Exception {
        List<String> result = Lists.newArrayList();
        int index = instances.indexOf(instanceId);
        int mod = tasks.size() % instances.size();
        int averageSize = tasks.size() <= instances.size() ? 1 : (mod > 0 && index < mod ? tasks.size() / instances.size() + 1 : tasks.size() / instances.size());
        int startIndex = (mod > 0 && index < mod) ? index * averageSize : index * averageSize + mod;
        int range = Math.min(averageSize, tasks.size() - startIndex);
        for (int i = 0; i < range; i++) {
            result.add(tasks.get((startIndex + i) % tasks.size()));
        }
        return result;
    }

}
