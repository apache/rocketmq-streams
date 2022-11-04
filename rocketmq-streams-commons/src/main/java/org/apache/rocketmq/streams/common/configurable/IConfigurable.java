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

import java.io.Serializable;
import java.util.Map;
import org.apache.rocketmq.streams.common.datatype.IJsonable;

/**
 * 对所有可配置的对象，做统一抽象，可配置的对象是：数据量不大，可以加载到内存，整个框架除了数据外，多数框架对象都是configuahle对象 统一抽象的好处是：可以统一的序列化，反序列化，存储，查询。 通过namespace隔离业务，通过type区分类型，namespace+type+name唯一标识一个对象。所有其他字段会被序列化到json_value中 继承BasedConfigurable 基类，会自动实现序列化和反序列化。也可以自己实现
 */
public interface IConfigurable extends IJsonable, IConfigurableIdentification, Serializable {

    /**
     * 把toJson的结果当作一个特殊属性
     */
    String JSON_PROPERTY = "configurable_json";

    /**
     * 把status当作configurable 的一个特殊属性
     */
    String STATUS_PROPERTY = "configurable_status";

    /**
     * 每个配置有一个独立的名字
     *
     * @param configureName configureName
     */
    void setConfigureName(String configureName);

    /**
     * 每个配置有独立的命名空间
     *
     * @param nameSpace namespace
     */
    void setNameSpace(String nameSpace);

    /**
     * 区分配置类型
     *
     * @param type type
     */
    void setType(String type);

    boolean init();

    void destroy();

    /**
     * 设置私有数据
     *
     * @param key   key
     * @param value value
     * @param <T>   t
     */
    <T> void putPrivateData(String key, T value);

    /**
     * 获取私有数据
     *
     * @param key key
     * @param <T> value
     * @return value
     */
    <T> T getPrivateData(String key);

    Map<String, Object> getPrivateData();

}
