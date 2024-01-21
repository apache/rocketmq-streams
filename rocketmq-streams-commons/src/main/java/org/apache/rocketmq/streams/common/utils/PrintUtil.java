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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IFieldProcessor;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.GenericParameterDataType;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;

public class PrintUtil {

    public static String LINE = System.getProperty("line.separator");
    private static boolean isSimpleModel = true;
    private static Set<String> excludeFieldNames = new HashSet<>();
    private static Map<String, String> excludeDefalutValueMap = new HashMap<>();

    static {
        excludeFieldNames.add("dbChannelName");
        excludeFieldNames.add("entityName");
        excludeFieldNames.add("channelName");
        excludeFieldNames.add("indexs");
        excludeFieldNames.add("ruleName");
        excludeFieldNames.add("msgMetaDataName");
        excludeFieldNames.add("actionNames");
        excludeFieldNames.add("scriptNames");
        excludeFieldNames.add("varNames");
        excludeFieldNames.add("expressionName");
        excludeFieldNames.add("expressionStr");

        excludeFieldNames.add("configureName");
        excludeFieldNames.add("nameSpace");
        excludeFieldNames.add("type");
        excludeFieldNames.add("version");
        excludeFieldNames.add("extendField");
        excludeDefalutValueMap.put("isJsonData", "true");
        excludeDefalutValueMap.put("msgIsJsonArray", "false");
        excludeDefalutValueMap.put("maxThread", "10");
        excludeDefalutValueMap.put("offset", "0");
        excludeDefalutValueMap.put("syncCount", "1000");
        excludeDefalutValueMap.put("syncTimeout", "60000");
        excludeDefalutValueMap.put("outputThreadCount", "-1");
        excludeDefalutValueMap.put("isAutoFlush", "false");
        excludeDefalutValueMap.put("batchSize", "6000");
        excludeDefalutValueMap.put("timeout", "60000");
        excludeDefalutValueMap.put("activtyTimeOut", "3000");
        excludeDefalutValueMap.put("startNow", "true");
        excludeDefalutValueMap.put("topic", "");
        excludeDefalutValueMap.put("pollingTime", "86400");
        excludeDefalutValueMap.put("closeSplitMode", "false");
        excludeDefalutValueMap.put("result", "while");
        excludeDefalutValueMap.put("ruleStatus", "3");
        excludeDefalutValueMap.put("isBatchMessage", "true");
        excludeDefalutValueMap.put("cancelAfterConfigurableRefreshListerner", "false");
    }

    public static void print(List<String> list) {
        for (String str : list) {
            System.out.println(str);
        }
    }

    public static void print(Map map) {
        if (map == null) {
            return;
        }
        Iterator<Map.Entry<String, Object>> it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String value = null;
            if (entry.getValue() != null) {
                value = entry.getValue().toString();
            }
            System.out.println(entry.getKey() + ":" + value);
        }
    }

    public static void print(Properties properties) {
        if (properties == null) {
            return;
        }
        Set<Object> set = properties.keySet();
        List<String> propertyKey = new ArrayList<>();
        for (Object object : set) {
            propertyKey.add(object.toString());
        }
        Collections.sort(propertyKey);
        Iterator<String> it = propertyKey.iterator();
        while (it.hasNext()) {
            String key = it.next();
            String value = properties.getProperty(key);
            System.out.println(key + "=" + value);
        }
    }

    public static void print(JSONObject jsonObject, String arrayFieldName) {
        JSONArray jsonArray = jsonObject.getJSONArray(arrayFieldName);
        print(jsonArray);
    }

    public static void print(JSONArray jsonArray) {
        if (jsonArray == null) {
            return;
        }
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject msg = jsonArray.getJSONObject(i);
            System.out.println(msg);
            ;
        }
    }

    public static String print(IConfigurable configurable, String... paras) {
        StringBuilder sb = new StringBuilder();
        if (paras != null && paras.length > 0 && StringUtil.isNotEmpty(paras[0])) {
            sb.append("#" + paras[0] + "##" + LINE);
        }
        if (paras != null && paras.length > 1 && StringUtil.isNotEmpty(paras[1])) {
            sb.append("name=" + paras[1] + LINE);
        }

        ReflectUtil.scanConfiguableFields(configurable, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                if (isSimpleModel && excludeFieldNames.contains(field.getName())) {
                    return;
                }

                DataType dataType = DataTypeUtil.createFieldDataType(o, field.getName());
                Object value = ReflectUtil.getBeanFieldValue(o, field.getName());
                if (value == null) {
                    return;
                }
                String valueStr = PrintUtil.getDataTypeStr(dataType, value);

                if (value != null) {
                    String excludeDefaultValue = excludeDefalutValueMap.get(field.getName());
                    if (isSimpleModel && excludeDefaultValue != null && excludeDefaultValue.equals(valueStr)) {
                        return;
                    }
                    if (field.getAnnotation(ENVDependence.class) != null && configurable instanceof BasedConfigurable) {
                        String oriValue = valueStr;
                        sb.append(field.getName() + "=" + oriValue + LINE);
                        String actualValue = (oriValue);
                        if (StringUtil.isNotEmpty(actualValue)) {
                            sb.append(field.getName() + "_mock_value=" + actualValue + LINE);
                        }
                    } else {
                        sb.append(field.getName() + "=" + valueStr + LINE);
                    }
                }

            }
        });
        return sb.toString();
    }

    public static String getDataTypeStr(DataType dataType, Object value) {
        if (GenericParameterDataType.class.isInstance(dataType)) {
            return ((GenericParameterDataType) dataType).toDataStr(value);
        }
        return dataType.toDataJson(value);
    }

    public static String print(ChainPipeline pipline) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("namespace.name", pipline.getNameSpace() + "." + pipline.getName());
        List<AbstractStage> stages = pipline.getStages();
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(pipline.getChannelName());
        for (AbstractStage stage : stages) {
            stringBuilder.append("--->");
            stringBuilder.append(stage.getName());
        }
        jsonObject.put("stages", stringBuilder.toString());
        return JsonableUtil.formatJson(jsonObject);
    }

}
