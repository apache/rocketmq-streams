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
package org.apache.rocketmq.streams.script.function.impl.math;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;

@Function
public class CardinalityFunction {

    /**
     * 返回一个集合中的元素数量
     *
     * @param message
     * @param context
     * @param objects
     * @return
     */
    @FunctionMethod(value = "cardinality", alias = "cardinality", comment = "返回一个集合中的元素数量")
    public Integer cardinality(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "Array", comment = "待求值的集合") Object[] objects) {
        Integer result = null;
        if (objects == null) {
            return result;
        }
        result = objects.length;
        return result;
    }

}
