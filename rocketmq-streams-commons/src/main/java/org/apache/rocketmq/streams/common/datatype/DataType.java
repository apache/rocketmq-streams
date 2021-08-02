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
package org.apache.rocketmq.streams.common.datatype;

import java.util.concurrent.atomic.AtomicInteger;

public interface DataType<T> extends IJsonable, DataJsonable<T> {

    /**
     * 范型参数
     */
    String GENERIC_PARAMETER = "genericParameter";

    /**
     * 当前类的classname
     */
    String DATA_TYPE_CLASS_NAME = "className";

    /**
     * @return 对应数据的原始class，比如list的子类,可能是list的子类
     */
    Class getDataClass();

    /**
     * 创建一个Datatype，由子类决定是否创建，如果没有任何成员变量，可以直接返回自己，如果有成员变量，需要创建一个新的
     *
     * @return
     */
    DataType create();

    /**
     * 代表类型的字符串描述，一般是类的simple类名
     *
     * @return
     */
    @Deprecated
    String getName();

    void setDataClazz(Class dataTypeClass);

    /**
     * 代表类型的字符串描述，一般是类的simple类名
     *
     * @return
     */
    String getDataTypeName();

    /**
     * 判断输入类型是否是自己支持的子类型
     *
     * @param clazz
     * @return
     */
    boolean matchClass(Class clazz);

    /**
     * 比如long可以代表bigint，这样，就需要完成支持的类型的类型转换
     *
     * @param object
     * @return
     */
    T convert(Object object);

    /**
     * 把值转换成字节数组，会被压缩，根据必须位返回值
     *
     * @param value
     * @return
     */
    byte[] toBytes(T value, boolean isCompress);

    /**
     * 把字节数组转换成具体数值
     *
     * @param bytes
     * @return
     */
    T byteToValue(byte[] bytes);

    /**
     * 把字节数组转换成具体数值，这种情况下，不支持压缩得到的byte数组
     *
     * @param bytes
     * @return
     */
    T byteToValue(byte[] bytes, AtomicInteger offset);

    /**
     * 把字节数组转换成具体数值，这种情况下，不支持压缩得到的byte数组
     *
     * @param bytes
     * @return
     */
    T byteToValue(byte[] bytes, int offset);
}
