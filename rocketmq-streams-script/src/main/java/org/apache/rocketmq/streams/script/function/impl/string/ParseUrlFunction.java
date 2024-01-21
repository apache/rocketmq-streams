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
package org.apache.rocketmq.streams.script.function.impl.string;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class ParseUrlFunction {
    @FunctionMethod(value = "parse_url", alias = "url", comment = "对url的解析，按key提取信息")
    public String parseurl(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "url") String urlStr,
        @FunctionParamter(value = "string", comment = "支持HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, 和 "
            + "USERINFO，不区分大小写,支持字段，常量") String part) throws MalformedURLException {
        return parseurl(message, context, urlStr, part, null);
    }

    /**
     * 对url的解析，按key提取信息
     *
     * @param message
     * @param context
     * @param urlStr
     * @param part
     * @param key
     * @return
     */
    @FunctionMethod(value = "parse_url", alias = "url", comment = "对url的解析，按key提取信息")
    public String parseurl(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "url") String urlStr,
        @FunctionParamter(value = "string", comment = "支持HOST, PATH, QUERY, REF, PROTOCOL, AUTHORITY, FILE, 和 "
            + "USERINFO，不区分大小写,支持字段，常量") String part,
        @FunctionParamter(value = "string", comment = "当part为QUERY时根据key的值取出在query string中的value值，否则忽略key参数,支持字段，常量")
        String key) throws MalformedURLException {
        if (StringUtil.isEmpty(urlStr) || StringUtil.isEmpty(part)) {
            return null;
        }
        urlStr = FunctionUtils.getValueString(message, context, urlStr);
        URL url = new URL(urlStr);
        part = FunctionUtils.getValueString(message, context, part);
        part = part.toUpperCase();
        if ("HOST".equals(part)) {
            return url.getHost();
        } else if ("PATH".equals(part)) {
            return url.getPath();
        } else if ("PROTOCOL".equals(part)) {
            return url.getProtocol();
        } else if ("REF".equals(part)) {
            return url.getRef();
        } else if ("PORT".equals(part)) {
            return url.getPort() + "";
        } else if ("AUTHORITY".equals(part)) {
            return url.getAuthority();
        } else if ("FIEL".equals(part)) {
            return url.getFile();
        } else if ("USERINFO".equals(part)) {
            return url.getUserInfo();
        } else if ("QUERY".equals(part)) {
            String query = url.getQuery();
            if (!StringUtil.isEmpty(key)) {
                key = FunctionUtils.getValueString(message, context, key);
            }
            if (StringUtil.isEmpty(key)) {
                return query;
            }
            String[] values = query.split("&");
            Map<String, String> map = new HashMap<>();
            for (String value : values) {
                String[] ex = value.split("=");
                map.put(ex[0], ex[1]);
            }
            return map.get(key);
        }
        return null;
    }
}
