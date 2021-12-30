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
