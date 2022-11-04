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
import com.google.common.collect.Maps;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.dispatcher.IStrategy;
import org.apache.rocketmq.streams.dispatcher.constant.DispatcherConstant;
import org.apache.rocketmq.streams.dispatcher.entity.DispatcherMapper;

public class LeastStrategy implements IStrategy {

    private DispatcherMapper currentDispatcherMapper = new DispatcherMapper();

    public DispatcherMapper getCurrentDispatcherMapper() {
        return currentDispatcherMapper;
    }

    public void setCurrentDispatcherMapper(DispatcherMapper currentDispatcherMapper) {
        this.currentDispatcherMapper = currentDispatcherMapper;
    }

    @Override public DispatcherMapper dispatch(List<String> tasks, List<String> instances) throws Exception {
        if (tasks == null || tasks.isEmpty() || instances == null || instances.isEmpty()) {
            return new DispatcherMapper();
        }

        int avg = 1;
        if (tasks.size() > instances.size()) {
            avg = tasks.size() / instances.size() + 1;
        }
        Set<String> currentInstances = this.currentDispatcherMapper.getInstances();
        List<String> needRemoveInstance = Lists.newArrayList();
        Map<String, String> needRemoveTask = Maps.newHashMap();
        for (String instance : currentInstances) {
            if (!instances.contains(instance)) {
                needRemoveInstance.add(instance);
            } else {
                Set<String> ts = this.currentDispatcherMapper.getTasks(instance);
                for (String s : ts) {
                    if (!tasks.contains(s)) {
                        needRemoveTask.put(s, instance);
                    }
                }
                if (ts.size() > avg) {//如果一个实例上的任务数超过平均值，也需要重新调度
                    Iterator<String> iterator = ts.iterator();
                    for (int i = 0; i < ts.size() - avg; i++) {
                        needRemoveTask.put(iterator.next(), instance);
                    }
                }

            }
        }
        for (String rm : needRemoveInstance) {
            this.currentDispatcherMapper.remove(rm);
        }
        for (Map.Entry<String, String> entry : needRemoveTask.entrySet()) {
            this.currentDispatcherMapper.removeTask(entry.getValue(), entry.getKey());
        }

        //已经调度的任务，不做改变
        List<String> needDispatcher = Lists.newArrayList();
        for (String task : tasks) {
            if (!this.currentDispatcherMapper.containTask(task)) {
                needDispatcher.add(task);
            }
        }

        for (String task : needDispatcher) {
            //每个需要调度的任务都需要选择合适的Instance
            Map<String, Integer> instanceSort = Maps.newHashMap();
            for (String instance : instances) {
                instanceSort.put(instance, this.currentDispatcherMapper.getTasks(instance).size());
            }
            List<Map.Entry<String, Integer>> list = Lists.newArrayList(instanceSort.entrySet());
            list.sort(Comparator.comparingInt(Map.Entry::getValue));

            String currectInstance = null;
            for (Map.Entry<String, Integer> entry : list) {
                //逐个遍历instance
                String instance = entry.getKey();
                Set<String> onlineTasks = this.currentDispatcherMapper.getTasks(instance);
                boolean avaliable = true;
                for (String onlineTask : onlineTasks) {
                    String taskPrefix = task.indexOf(DispatcherConstant.TASK_SERIAL_NUM_SEPARATOR) > 0 ? task.substring(0, task.indexOf(DispatcherConstant.TASK_SERIAL_NUM_SEPARATOR)) : task;
                    String onlineTaskPrefix = onlineTask.indexOf(DispatcherConstant.TASK_SERIAL_NUM_SEPARATOR) > 0 ? onlineTask.substring(0, onlineTask.indexOf(DispatcherConstant.TASK_SERIAL_NUM_SEPARATOR)) : onlineTask;
                    //如果该instance包含的任务有和当前任务前缀相同的， 那该instance就不可以运行该任务
                    if (onlineTaskPrefix.equals(taskPrefix)) {
                        avaliable = false;
                        break;
                    }
                }
                if (avaliable) {
                    currectInstance = instance;
                    break;
                }
            }
            if (currectInstance != null) {
                this.currentDispatcherMapper.putTask(currectInstance, task);
            }

        }
        return this.currentDispatcherMapper;
    }

    public static void main(String[] args) {
        List<String> tasks = Lists.newArrayList("cloud_siem_vpc_slb7l_log_1335759343513432_0$#0",
            "cloud_siem_aegis_vul_log_0$#0", "cloud_siem_aegis_crack_from_beaver_0$#0", "cloud_siem_aegis_proc_snapshot_0$#0",
            "cloud_siem_polardb1x_audit_1154554141560953_0$#0", "cloud_siem_aegis_proc_0$#0",
            "cloud_siem_aegis_dns_0$#0",
            "cloud_siem_cfw_flow_1335759343513432_0$#0",
            "cloud_siem_polardb2x_audit_1335759343513432_0$#0",
            "cloud_siem_aegis_health_check_0$#0",
            "cloud_siem_vpc_flow_log_1335759343513432_0$#0",
            "cloud_siem_aegis_netstat_0$#0",
            "cloud_siem_polardb2x_audit_1154554141560953_0$#0",
            "cloud_siem_aegis_sas_alert_0$#0",
            "cloud_siem_waf_flow_1335759343513432_0$#0",
            "cloud_siem_oss_access_log_1335759343513432_0$#0",
            "cloud_siem_vpc_eni_log_1335759343513432_0$#0",
            "cloud_siem_aegis_login_event_0$#0",
            "cloud_siem_cfw_alert_1335759343513432$#0",
            "cloud_siem_polardb1x_audit_1335759343513432_0$#0",
            "cloud_siem_aegis_netinfo_snapshot_0$#0",
            "cloud_siem_cfw_alert_1335759343513432_0$#0");
        List<String> instances = Lists.newArrayList("10_219_220_145_4129", "10_219_223_109_6190", "10_219_220_103_5984", "10_219_220_106_6049");
        LeastStrategy leastStrategy = new LeastStrategy();
        DispatcherMapper dispatcherMapper = DispatcherMapper.parse("{\n" +
            "  \"10_219_220_145_4129\": [\n" +
            "    \"cloud_siem_polardb2x_audit_1335759343513432_0$#0\",\n" +
            "    \"cloud_siem_vpc_eni_log_1335759343513432_0$#0\",\n" +
            "    \"cloud_siem_vpc_flow_log_1335759343513432_0$#0\",\n" +
            "    \"cloud_siem_vpc_slb7l_log_1335759343513432_0$#0\",\n" +
            "    \"cloud_siem_waf_flow_1335759343513432_0$#0\"\n" +
            "  ],\n" +
            "  \"10_219_220_106_6050\": [\n" +
            "    \"cloud_siem_aegis_login_event_0$#0\",\n" +
            "    \"cloud_siem_aegis_netstat_0$#0\",\n" +
            "    \"cloud_siem_aegis_proc_0$#0\",\n" +
            "    \"cloud_siem_aegis_vul_log_0$#0\",\n" +
            "    \"cloud_siem_cfw_alert_1335759343513432_0$#0\"\n" +
            "  ],\n" +
            "  \"10_219_223_109_6190\": [\n" +
            "    \"cloud_siem_aegis_crack_from_beaver_0$#0\",\n" +
            "    \"cloud_siem_aegis_dns_0$#0\",\n" +
            "    \"cloud_siem_cfw_alert_1335759343513432$#0\",\n" +
            "    \"cloud_siem_polardb2x_audit_1154554141560953_0$#0\"\n" +
            "  ],\n" +
            "  \"10_219_220_103_5984\": [\n" +
            "    \"cloud_siem_aegis_proc_snapshot_0$#0\",\n" +
            "    \"cloud_siem_aegis_sas_alert_0$#0\",\n" +
            "    \"cloud_siem_cfw_flow_1335759343513432_0$#0\",\n" +
            "    \"cloud_siem_polardb1x_audit_1335759343513432_0$#0\"\n" +
            "  ],\n" +
            "  \"10_219_220_106_6049\": [\n" +
            "    \"cloud_siem_aegis_health_check_0$#0\",\n" +
            "    \"cloud_siem_aegis_netinfo_snapshot_0$#0\",\n" +
            "    \"cloud_siem_oss_access_log_1335759343513432_0$#0\",\n" +
            "    \"cloud_siem_polardb1x_audit_1154554141560953_0$#0\"\n" +
            "  ]\n" +
            "}");
        leastStrategy.setCurrentDispatcherMapper(dispatcherMapper);

        try {
            DispatcherMapper newDispatcher = leastStrategy.dispatch(tasks, instances);
            System.out.println(newDispatcher);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

//        int i = 0;
//        for (String task : tasks) {
//            try {
//                DispatcherMapper newDispatcher = leastStrategy.dispatch(Lists.newArrayList(task), instances);
//                i++;
//                for (String instance : instances) {
//                    System.out.println("第" + i + "波" + instance + ":" + newDispatcher.getTasks(instance));
//                }
//                leastStrategy.setCurrentDispatcherMapper(newDispatcher);
//            } catch (Exception e) {
//                throw new RuntimeException(e);
//            }
//        }
    }
}
