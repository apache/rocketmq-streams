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
package org.apache.rocketmq.streams.common.configurable;

public interface IConfigurableIdentification {
    /**
     * 配置的名字，同一个配置空间名字必须唯一
     *
     * @return 配置项名称
     */
    String getName();

    /**
     * 用于区分不同的业务，用命名空间隔离业务
     *
     * @return 配置项命名空间
     */
    String getNameSpace();

    /**
     * 配置对应的type
     *
     * @return 配置项类型
     */
    String getType();
}
