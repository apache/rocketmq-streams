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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * * 主要用于展示
 */
public class FunctionInfoMap {

    private static final String ZERO_PARAMETER = "zero";

    private Set<FunctionInfo> functionConfigureList = new HashSet<>();

    //    public boolean contains(String functionName,Object bean,Method method){
    //        Map<String,FunctionConfigure> kv=functionConfigureMap.get(functionName);
    //        if(kv==null){
    //            return false;
    //        }
    //        FunctionConfigure engine = new FunctionConfigure(functionName, method, bean);
    //        String methodSign=createParamterSign(engine.getParameterDataTypes());
    //        return kv.containsKey(methodSign);
    //    }

    @Deprecated
    public FunctionInfo getSigleFunction() {
        return functionConfigureList.iterator().next();
    }

    public FunctionInfo getFunction(String jsonConfigure, Object... dataParameters) {

        if (functionConfigureList != null && functionConfigureList.size() == 1) {
            return functionConfigureList.iterator().next();
        }
        Iterator<FunctionInfo> it = functionConfigureList.iterator();
        while (it.hasNext()) {
            FunctionInfo configure = it.next();
            //            Object[] parameters=configure.getRealParameters(jsonConfigure,dataParameters);
            //            if(configure.matchMethod(parameters)){
            return configure;
            //            }
        }
        return null;
    }

    public FunctionInfo getFunction(Object... parameters) {
        if (functionConfigureList != null && functionConfigureList.size() == 1) {
            return functionConfigureList.iterator().next();
        }
        Iterator<FunctionInfo> it = functionConfigureList.iterator();
        while (it.hasNext()) {
            FunctionInfo configure = it.next();
            //            if(configure.matchMethod(parameters)){
            return configure;
            //            }
        }
        return null;
    }

    public void registFunction(FunctionInfo engine) {
        functionConfigureList.add(engine);
    }

    public Set<FunctionInfo> getFunctionConfigureList() {
        return this.functionConfigureList;
    }
}
