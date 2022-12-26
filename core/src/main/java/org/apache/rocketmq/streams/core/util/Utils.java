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
package org.apache.rocketmq.streams.core.util;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.common.Constant;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String pattern = "%s@%s@%s";
    static {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static String buildKey(String brokerName, String topic, int queueId) {
        return String.format(pattern, brokerName, topic, queueId);
    }

    public static String buildKey(String key, String... args) {
        if (StringUtils.isEmpty(key)) {
            return null;
        }

        StringBuilder builder = new StringBuilder();
        builder.append(key);

        if (args == null || args.length == 0) {
            return builder.toString();
        }

        builder.append(Constant.SPLIT);
        for (String arg : args) {
            builder.append(arg);
            builder.append(Constant.SPLIT);
        }

        return builder.substring(0, builder.lastIndexOf(Constant.SPLIT));
    }

    public static String[] split(String source) {
        return split(source, Constant.SPLIT);
    }

    public static String[] split(String source, String split) {
        if (StringUtils.isEmpty(source) || StringUtils.isEmpty(split)) {
            return new String[]{};
        }

        return source.split(split);
    }

    public static byte[] object2Byte(Object target) throws JsonProcessingException {
        if (target == null) {
            return new byte[]{};
        }

        return objectMapper.writeValueAsBytes(target);
    }


    public static <B> B byte2Object(byte[] source, Class<B> clazz) throws IOException {
        if (source == null || source.length ==0 || clazz == null) {
            return null;
        }

        return objectMapper.readValue(source, clazz);
    }

    public static <B> B byte2Object(byte[] source, TypeReference<B> valueTypeRef) throws IOException {
        if (source == null || source.length ==0 || valueTypeRef == null) {
            return null;
        }

        return objectMapper.readValue(source, valueTypeRef);
    }

    public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static String format(long timestamp) {
        Date date = new Date(timestamp);
        return df.format(date);
    }

    public static String toHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }


}
