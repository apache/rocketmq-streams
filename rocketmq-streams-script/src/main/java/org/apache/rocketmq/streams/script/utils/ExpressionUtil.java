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
package org.apache.rocketmq.streams.script.utils;

import java.util.List;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class ExpressionUtil {

    public static boolean expressionEquals(IScriptExpression ori,IScriptExpression dest){
        if(!ori.getFunctionName().equals(dest.getFunctionName())){
            return false;
        }
        List<IScriptParamter> oriScriptParamters= ori.getScriptParamters();
        List<IScriptParamter> destScriptParameters=dest.getScriptParamters();
        if(oriScriptParamters==null&&destScriptParameters!=null){
            return false;
        }
        if(oriScriptParamters!=null&&destScriptParameters==null){
            return false;
        }
        if(oriScriptParamters==null&&destScriptParameters==null){
            return true;
        }
        if(oriScriptParamters.size()!=destScriptParameters.size()){
            return false;
        }

        for(int i=0;i<oriScriptParamters.size();i++){
            IScriptParamter oriParameter=oriScriptParamters.get(i);
            IScriptParamter destParameter=destScriptParameters.get(i);
            if(!oriParameter.getClass().getName().equals(destParameter.getClass().getName())){
                return false;
            }
            if(!oriParameter.getScriptParameterStr().equals(destParameter.getScriptParameterStr())){
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        FunctionScript functionScript=new FunctionScript("a=sum(b);");
        functionScript.init();
        FunctionScript functionScript1=new FunctionScript("x=sum(b);");
        functionScript1.init();
        System.out.println(expressionEquals(functionScript.getScriptExpressions().get(0),functionScript1.getScriptExpressions().get(0)));
    }
}
