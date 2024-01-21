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
package org.apache.rocketmq.streams.dispatcher;

import java.util.List;
import java.util.Map;

public interface IStrategyCallBack {

    /**
     * 获取需要调度的任务名称列表
     *
     * @return 需要调度的任务名称列表
     */
    List<String> list();

    /**
     * 相同groupname的task，允许共享实例，非相同groupname的task 不允许共享资源
     *
     * @return
     */
    Map<String, String> getTaskGroupName();

    /**
     * 一个实例最大的任务数，如果返回-1，表示不限制
     *
     * @return
     */
    int getInstanceMaxTaskCount();

}
