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
package org.apache.rocketmq.streams.script.function.impl.relation;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;

@Function
public class AndFunction {

    public static void main(String[] args) {
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JSONObject msg = new JSONObject();
        msg.put("name", "chris");
        msg.put("age", 18);

        scriptComponent.getService().executeScript(msg, "x=or(equals(name,'chris'),>(age,19),!(equals(age,1))))");
        System.out.println(msg);
    }

    @FunctionMethod(value = "and", alias = "&", comment = "支持内嵌函数")
    public Boolean and(Boolean... values) {
        if (values == null) {
            return false;
        }
        for (Boolean value : values) {
            if (value != null && value) {
                continue;
            }
            return false;
        }
        return true;

    }

    @FunctionMethod(value = "or", alias = "|", comment = "支持内嵌函数")
    public Boolean or(Boolean... values) {
        if (values == null) {
            return false;
        }
        for (Boolean value : values) {
            if (value != null && value) {
                return true;
            }
        }
        return false;

    }
}
