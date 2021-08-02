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
package org.apache.rocketmq.streams.script.function.service;

/**
 * 目前主要用在URLPythonScript 中，后续不推荐使用。 在后续版本会删除这个接口
 */
@Deprecated
public interface IDipperInterfaceAdpater {
    /**
     * 获取第n个参数名称
     *
     * @param index
     * @return
     */
    String getParamterName(int index);

    /**
     * 一共几个参数
     *
     * @return
     */
    int count();

    /**
     * Python 转 Java 基础类
     *
     * @param param
     * @return Object
     */
    Object doScript(Object param);
}
