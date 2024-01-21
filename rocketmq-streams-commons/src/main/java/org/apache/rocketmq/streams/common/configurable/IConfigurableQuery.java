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

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface IConfigurableQuery {
    /**
     * 根据类型查询，同类的configuable对象
     *
     * @param type
     * @param <T>
     * @return
     */
    <T extends IConfigurable> List<T> queryConfigurableByType(String type);

    <T> T queryConfigurable(String configurableType, String name);

    List<IConfigurable> queryConfigurable(String type);

    <T> Map<String, T> queryConfigurableMapByType(String type);

    /**
     * 获取所有的数据
     *
     * @return
     */
    Collection<IConfigurable> findAll();

}
