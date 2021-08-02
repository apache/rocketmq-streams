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
package org.apache.rocketmq.streams.script.function.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.streams.common.datatype.DataType;

public class FunctionConfigureMap {

    private static final String ZERO_PARAMETER = "zero";

    private List<FunctionConfigure> functionConfigureList = new ArrayList<>();

    private Map<Integer, List<FunctionConfigure>> paramterNumber2Configures = new HashMap<>();

    private List<FunctionConfigure> varParameterConfigures = new ArrayList<>();

    /**
     * 所有的FunctionConfigure都是startwithcontent，设置为true，否则为false
     */
    private boolean startWithContext = true;

    /**
     * 根据参数查找FunctionConfigure，先根据个数找，然后做参数类型判断
     *
     * @param parameters
     * @return
     */
    public FunctionConfigure getFunction(Object... parameters) {
        Integer length = parameters == null ? 0 : parameters.length;
        List<FunctionConfigure> configures = paramterNumber2Configures.get(length);
        if (configures != null && configures.size() == 1) {
            FunctionConfigure functionConfigure = configures.get(0);
            boolean success = functionConfigure.matchMethod(parameters);
            if (success) {
                return functionConfigure;
            }
        }
        if (configures == null) {
            configures = functionConfigureList;
        }
        Iterator<FunctionConfigure> it = configures.iterator();
        while (it.hasNext()) {
            FunctionConfigure configure = it.next();
            if (configure.matchMethod(parameters)) {
                return configure;
            }
        }
        return null;
    }

    public void registFunction(FunctionConfigure engine) {
        if (engine.isStartWithContext() == false) {
            this.startWithContext = false;
        }

        functionConfigureList.add(engine);
        if (engine.isVariableParameter()) {
            varParameterConfigures.add(engine);
            return;
        }
        DataType[] dataTypes = engine.getParameterDataTypes();
        Integer length = dataTypes == null ? 0 : dataTypes.length;
        List<FunctionConfigure> configures = paramterNumber2Configures.get(length);
        if (configures == null) {
            configures = new ArrayList<>();
            paramterNumber2Configures.put(length, configures);
        }
        configures.add(engine);
    }

    /**
     * 逻辑有问题，可能有多个context开头的方法
     *
     * @param classes
     * @return
     */
    public boolean startWith(Class[] classes) {
        if (functionConfigureList == null) {
            return false;
        }
        for (FunctionConfigure functionConfigure : functionConfigureList) {
            if (functionConfigure.startWith(classes)) {
                return true;
            }
        }
        return false;
    }

}
