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
package org.apache.rocketmq.streams.openapi;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.streams.common.interfaces.IDim;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.utils.ConfigurableUtil;
import org.apache.rocketmq.streams.dim.builder.IDimSQLParser;

@AutoService(IDimSQLParser.class)
@ServiceName(value = OpenAPISQLParser.TYPE, aliasName = "openapi")
public class OpenAPISQLParser implements IDimSQLParser {
    public static final String TYPE = "openapi";

    protected static Map<String, String> formatNames = new HashMap<>();

    static {
        formatNames.put("region", "regionId");
        formatNames.put("ak", "accessKeyId");
        formatNames.put("sk", "accessSecret");
        formatNames.put("product", "productName");
        formatNames.put("query", "defaultParameterStr");
        formatNames.put("version", "actionVersion");
    }

    @Override public IDim parseDim(String namespace, Properties properties, MetaData metaData) {
        JSONObject paras = new JSONObject();
        Iterator<Object> it = properties.keySet().iterator();
        while (it.hasNext()) {
            String key = (String) it.next();
            Object value = properties.get(key);
            if (formatNames.containsKey(key)) {
                key = formatNames.get(key);
            }
            if ("type".equals(key)) {
                continue;
            }
            paras.put(key, value);
        }
        OpenAPIDim dim = (OpenAPIDim) ConfigurableUtil.create(OpenAPIDim.class.getName(), namespace, null, paras, null);
        return dim;
    }
}
