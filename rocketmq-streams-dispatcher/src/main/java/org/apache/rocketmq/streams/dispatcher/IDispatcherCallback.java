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

public interface IDispatcherCallback<T> {

    /**
     * 启动任务
     *
     * @param jobNames 需要启动的任务名称列表
     * @return 启动成功的任务名称列表
     */
    List<String> start(List<String> jobNames);

    /**
     * 停止任务
     *
     * @param jobNames 需要停止的任务名称列表
     * @return 停止成功的任务名称列表
     */
    List<String> stop(List<String> jobNames);

    /**
     * 获取需要调度的任务名称列表
     *
     * @return 需要调度的任务名称列表
     */
    List<String> list();

    /**
     * callback 销毁
     */
    void destroy();

}
