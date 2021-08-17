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
package org.apache.rocketmq.streams.script.service;

/**
 * 主要用于blink udf转化，blink udf最终会转化成一个script的子类，如果实现了这个接口。在init时，会调用映射的初始化方法名称 blink udf 映射的是open方法
 */
public interface IScriptUDFInit {

    /**
     * 初始化的方法名
     *
     * @return
     */
    String getInitMethodName();

    /**
     * udf/udtf对应的对象实例
     *
     * @return
     */
    Object getInstance();
}
