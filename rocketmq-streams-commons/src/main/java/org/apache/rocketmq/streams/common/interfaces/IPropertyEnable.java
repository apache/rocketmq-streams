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
package org.apache.rocketmq.streams.common.interfaces;

import java.util.Properties;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

/**
 * 可以通过属性文件创建的对象，主要应用在创建configuableserivce实现类，可以通过配置文件获取创建组件的数据
 */
public interface IPropertyEnable {

    static <T extends IPropertyEnable> T create(String className, Properties properties) {
        IPropertyEnable propertyEnable = ReflectUtil.forInstance(className);
        propertyEnable.initProperty(properties);
        return (T) propertyEnable;
    }

    static <T extends IPropertyEnable> T create(Class claszz, Properties properties) {
        return create(claszz.getName(), properties);
    }

    void initProperty(Properties properties);
}
