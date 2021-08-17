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
package org.apache.rocketmq.streams.script.function.impl.filter;

import org.apache.rocketmq.streams.script.annotation.Function;

@Function
@Deprecated
public class FilterFunction {

    //    @FunctionMethod(value = "filter", comment = "建议用if(fieldName==value)then{break();}方式替代")
    //    @Deprecated
    //    public boolean filter(IMessage message, FunctionContext context, String fieldName, String functionName,
    //        String value) {
    //        Boolean match = (Boolean) context.executeFunction(functionName, message, fieldName, value);
    //        if (match) {
    //            context.breakExecute();
    //            return true;
    //        }
    //        return false;
    //
    //    }
    //
    //    @FunctionMethod(value = "notfilter", comment = "建议用if(fieldName!=value)then{break();}方式替代")
    //    public boolean notfilter(IMessage message, FunctionContext context, String fieldName, String functionName,
    //        String value) {
    //        Boolean match = (Boolean) context.executeFunction(functionName, message, fieldName, value);
    //        if (!match) {
    //            context.breakExecute();
    //            return true;
    //        }
    //        return false;
    //
    //    }
    //
    //    @FunctionMethod(value = "filterIn", comment = "建议用if(in(fieldName,value1,value2,value3...))then{break();}方式替代")
    //    public boolean filterIn(IMessage message, FunctionContext context, String fieldName, String functionName,
    //        String... value) {
    //        Boolean match = (Boolean) context.executeFunction(functionName, message, fieldName, value);
    //        if (match) {
    //            context.breakExecute();
    //            return true;
    //        }
    //        return false;
    //
    //    }
    //
    //    @FunctionMethod(value = "notfilterIn", comment = "建议用if(in(fieldName,value1,value2,value3...)==false)then{break()"
    //        + ";}方式替代")
    //    public boolean notfilterIn(IMessage message, FunctionContext context, String fieldName, String functionName,
    //        String... value) {
    //        Boolean match = (Boolean) context.executeFunction(functionName, message, context, fieldName, value);
    //        if (!match) {
    //            context.breakExecute();
    //            return true;
    //        }
    //        return false;
    //
    //    }
}
