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
package org.apache.rocketmq.streams.window.model;

import java.util.List;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;

public class FunctionExecutor {

    private static final String KEY = "key";

    /**
     * the computed column defined by user or system
     */
    private String column;

    /**
     * the executor of operator(column=function(xxx))
     */
    private IStreamOperator<IMessage, List<IMessage>> executor;

    public FunctionExecutor(String column, IStreamOperator<IMessage, List<IMessage>> executor) {
        this.column = column;
        this.executor = executor;
    }

    public String getColumn() {
        return column;
    }

    public IStreamOperator<IMessage, List<IMessage>> getExecutor() {
        return executor;
    }

}
