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

import java.io.Serializable;

/**
 * UDAF的标准接口，所有的udaf都要实现这个接口。 Blink UDAF也是生成这个接口的实现类来完成转化的
 */
public interface IAccumulator<T, ACC> extends Serializable {

    String ACCUMULATOR_VALUE = "value";

    /**
     * 初始化AggregateFunction的accumulator，系统在第一个做aggregate计算之前调用一次这个方法
     *
     * @return
     */
    ACC createAccumulator();

    /**
     * 系统在每次aggregate计算完成后调用这个方法
     *
     * @param accumulator
     * @return
     */
    T getValue(ACC accumulator);

    /**
     * accumulate
     *
     * @param accumulator
     * @param parameters
     */
    void accumulate(ACC accumulator, Object... parameters);

    /**
     * merge
     *
     * @param accumulator
     * @param its
     */
    void merge(ACC accumulator, Iterable<ACC> its);

    /**
     * retract
     *
     * @param accumulator
     * @param parameters
     */
    void retract(ACC accumulator, String... parameters);

}
