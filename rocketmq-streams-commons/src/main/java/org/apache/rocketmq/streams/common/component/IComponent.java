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
package org.apache.rocketmq.streams.common.component;

/**
 * 对组件的封装，隐藏组件的实现细节，组件的使用更简单
 */
public interface IComponent<T> {

    /**
     * 通过默认的配置加载组件，默认配置存放的路径为component／组件名称.default.properties
     *
     * @return
     */
    boolean init();

    /**
     * 启动组建，用到的服务需要进行初始化，默认逻辑是谁创建，谁初始化
     *
     * @return
     */
    boolean start(String namespace);

    /**
     * 关闭组建
     *
     * @return
     */
    boolean stop();

    /**
     * 获取组件对应的服务
     *
     * @return
     */
    T getService();

    /**
     * 覆盖init()属性中的内容，形式为key:value,可以多组
     *
     * @param kvs
     * @return
     */
    @Deprecated
    boolean initByPropertiesStr(String... kvs);

    /**
     * 加载文件初始化组件，如果是spring文件，同factory,如果是属性文件，同init，会对新创建的服务进行初始化。
     *
     * @param classPath
     * @return
     */
    @Deprecated
    boolean initByClassPath(String classPath);

    @Deprecated
    boolean initByFilePath(String filePath);

}
