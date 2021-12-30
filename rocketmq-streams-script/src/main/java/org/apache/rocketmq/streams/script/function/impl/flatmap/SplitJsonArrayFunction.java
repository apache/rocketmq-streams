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
package org.apache.rocketmq.streams.script.function.impl.flatmap;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;

public class SplitJsonArrayFunction {

    public List<Map<String, Object>> eval(JSONArray jsonArray, String... fieldNames) {
        List<Map<String, Object>> result = Lists.newArrayList();
        if (!jsonArray.isEmpty()) {
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject jsonObject = jsonArray.getJSONObject(i);
                if (fieldNames.length == 0) {
                    result.add(jsonObject);
                } else {
                    Map<String, Object> data = Maps.newHashMap();
                    int j = 0;
                    for (String fieldName : fieldNames) {
                        data.put("f" + (j++), jsonObject.get(fieldName));
                    }

                    result.add(data);
                }
            }
        }
        return result;
    }

}
