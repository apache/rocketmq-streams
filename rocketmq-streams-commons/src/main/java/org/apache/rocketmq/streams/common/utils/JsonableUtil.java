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
package org.apache.rocketmq.streams.common.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;

@SuppressWarnings("rawtypes")
public class JsonableUtil {

    private static final DataType stringDataType = new StringDataType();
    private static final DataType<List> listDataType = new ListDataType(List.class, stringDataType);
    private static final DataType<Map> mapDataType = new MapDataType(Map.class, stringDataType, stringDataType);
    private static Gson gson = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();

    public static String toJson(List<String> value) {
        return listDataType.toDataJson(value);
    }

    @SuppressWarnings("unchecked")
    public static List<String> getListData(String value) {
        return listDataType.getData(value);
    }

    public static String toJson(Map<String, String> value) {
        return mapDataType.toDataJson(value);
    }

    @SuppressWarnings("unchecked")
    public static Map<String, String> getMapData(String value) {
        return mapDataType.getData(value);
    }

    public static String formatJson(JSONObject jsonObject) {
        String value = gson.toJson(jsonObject);
        return value;
    }

    public static String formatJson(JSONArray jsonObject) {
        String value = gson.toJson(jsonObject);
        return value;
    }
}
