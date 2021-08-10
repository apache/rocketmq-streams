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
package org.apache.rocketmq.streams.serviceloader;

import java.util.List;

public interface IServiceLoaderService<T> {

    /**
     * 获取某个指定名称的服务对象
     *
     * @param name
     * @param
     * @return
     */
    T loadService(String name);

    /**
     * 获取多个实现类
     *
     * @param
     * @return
     */
    List<T> loadService();

    /**
     * 如果forceRefresh＝＝false, refresh多次调用只会执行一次； 如果forceRefresh==true,每次调用都会做一次重新扫描
     *
     * @param forceRefresh
     */
    void refresh(boolean forceRefresh);

}
