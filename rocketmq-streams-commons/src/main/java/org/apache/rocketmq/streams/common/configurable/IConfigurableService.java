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

/**
 * 对Configurable对象，做统一的管理，统一查询，插入和更新。 insert/update 把configuabel对象写入存储，支持文件存储（file），内存存储（memory）和db存储（DB）。可以在配置通过这个ConfigureFileKey.CONNECT_TYPE key 配置 query 是基于内存的查询，对象定时load到内存，可以在属性文件通过这个ConfigureFileKey.POLLING_TIME key配置加载周期，单位是秒 新对象加载后生效，已经存在的对象只有updateFlag发生变化才会被替换
 */
public interface IConfigurableService {

    String CLASS_NAME = "className";

    String CLASS_TYPE = "classType";

    /**
     * 顶层的命名空间
     */
    String PARENT_CHANNEL_NAME_SPACE = "rocketmq.streams.root.namespace";

    /**
     * 超顶层的命名空间
     */
    String ROOT_CHANNEL_NAME_SPACE = "rocketmq_streams_root_namespace";

    String DEFAULT_SERVICE_NAME = "DB";

    String MEMORY_SERVICE_NAME = "memory";

    String FILE_SERVICE_NAME = "file";

    String FILE_PATH_NAME = "filePathAndName";

    String HTTP_SERVICE_NAME = "http";

    String HTTP_SERVICE_ENDPOINT = "dipper.configurable.service.type.http.endpoint";


    /**
     * 启动定时任务，定期从存储加载对象到内存
     *
     * @param namespace
     */
    void initConfigurables(String namespace);

    /**
     * 从存储加载对象到内存
     *
     * @param namespace
     * @return
     */
    boolean refreshConfigurable(String namespace);

    /**
     * 根据type进行配置查询
     *
     * @return
     */
    @Deprecated
    List<IConfigurable> queryConfigurable(String type);

    /**
     * 根据类型查询，同类的configuable对象
     *
     * @param type
     * @param <T>
     * @return
     */
    <T extends IConfigurable> List<T> queryConfigurableByType(String type);

    /**
     * 根据type和name进行配置查询
     *
     * @return
     */
    @Deprecated
    IConfigurable queryConfigurableByIdent(String type, String name);

    /**
     * 共享实现中：根据namespace,type和name进行配置查询 单业务实现中：根据type和name进行配置查询
     *
     * @param identification namespace,type;name
     * @return
     */
    @Deprecated
    IConfigurable queryConfigurableByIdent(String identification);

    /**
     * 有具体的存储子类实现，直接写数据到存储，不会更新缓存，所以insert后，直接查询会查询不到，必须再次加载后才能获取
     *
     * @param configurable
     */
    void insert(IConfigurable configurable);

    /**
     * 有具体的存储子类实现，直接写数据到存储，不会更新缓存，所以insert后，直接查询会查询不到，必须再次加载后才能获取
     *
     * @param configurable
     */
    void update(IConfigurable configurable);

    <T> Map<String, T> queryConfigurableMapByType(String type);

    <T> T queryConfigurable(String configurableType, String name);

    String getNamespace();

    /**
     * 获取所有的数据
     *
     * @return
     */
    Collection<IConfigurable> findAll();

    /**
     * 从存储加载configurable对象，此处加载完，不会改变状态，query接口是无法查询的
     *
     * @param type
     * @return
     */
    <T extends IConfigurable> List<T> loadConfigurableFromStorage(String type);

}
