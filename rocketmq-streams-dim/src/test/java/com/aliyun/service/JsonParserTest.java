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
package com.aliyun.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

public class JsonParserTest {

    @Test
    public void testJson() {
        String array =
            "[{\"value\":\"groupname\",\"key\":\"group_name\"},{\"value\":\"username\",\"key\":\"user_name\"},"
                + "{\"value\":\"seq\",\"key\":\"index\"},{\"value\":\"egroupid\",\"key\":\"egroup_id\"},"
                + "{\"value\":\"filepath\",\"key\":\"file_path\"},{\"value\":\"groupid\",\"key\":\"group_id\"},"
                + "{\"value\":\"pfilename\",\"key\":\"pfile_path\"},{\"value\":\"safe_mode\",\"key\":\"perm\"},"
                + "{\"value\":\"cmdline\",\"key\":\"cmd_line\"}]";
        String jsonStr =
            "{\"className\":\"com.aliyun.filter.result.FieldReNameScript\",\"oldField2NewFiled\":" + array + "}";
        JSONArray jsonObject = JSON.parseArray(array);
        JSONObject js = JSON.parseObject(jsonStr);
        System.out.println(js.toJSONString());
    }
}
