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
package org.apache.rocketmq.streams.script.function.aggregation;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.datatype.IJsonable;

public class SumAccum implements IJsonable {
    public Number sum;

    public Number getSum() {
        return sum;
    }

    public void setSum(Number sum) {
        this.sum = sum;
    }

    @Override public String toJson() {
        JSONObject msg = new JSONObject();
        msg.put("sum", sum);
        return msg.toJSONString();
    }

    @Override public void toObject(String jsonString) {
        if (jsonString == null) {
            return;
        }
        JSONObject msg = JSONObject.parseObject(jsonString);
        sum = msg.getDouble("sum");

    }
}
