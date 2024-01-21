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
package org.apache.rocketmq.streams.dim.service.impl;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.interfaces.IDim;
import org.apache.rocketmq.streams.dim.service.IDimService;

public class DimServiceImpl implements IDimService {

    private IDim nameList;

    /**
     * 传入要比对的字段，进行规则匹配。字段和名单的比对逻辑，写在规则中
     *
     * @param msgs 字段名默认为数组的索引，如1，2，3
     * @return
     */
    @Override
    public Map<String, Object> match(String dimName, String expressionStr, Object... msgs) {
        if (msgs == null || msgs.length == 0) {
            return null;
        }
        int i = 0;
        JSONObject jsonObject = new JSONObject();
        for (Object o : msgs) {
            jsonObject.put(i + "", o);
            i++;
        }
        return match(dimName, expressionStr, jsonObject);
    }

    @Override
    public List<Map<String, Object>> matchSupportMultiRow(String dimName, String expressionStr, Map<String, Object> msgs) {
        return matchSupportMultiRow(dimName, expressionStr, msgs, null);
    }

    //ewewwew
    @Override
    public List<Map<String, Object>> matchSupportMultiRow(String dimName, String expressionStr, Map<String, Object> msgs, String script) {
        JSONObject jsonObject = createJsonable(msgs);
        if (nameList != null) {
            return nameList.matchExpression(expressionStr, jsonObject, true, script);
        } else {
            return null;
        }
    }

    ///sdsasdasds
    @Override
    public Map<String, Object> match(String nameListName, String expressionStr, Map<String, Object> parameters) {
        JSONObject jsonObject = createJsonable(parameters);
        if (nameList != null) {
            return nameList.matchExpression(expressionStr, jsonObject);
        } else {
            return null;
        }
    }

    private JSONObject createJsonable(Map<String, Object> parameters) {
        JSONObject jsonObject = null;
        if (parameters instanceof JSONObject) {
            jsonObject = (JSONObject) parameters;
        } else {
            jsonObject.putAll(parameters);
        }
        return jsonObject;
    }

    public IDim getNameList() {
        return nameList;
    }

    public void setNameList(IDim nameList) {
        this.nameList = nameList;
    }
}
