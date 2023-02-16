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


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.exception.RStreamsException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Utils {
    private static final ObjectMapper objectMapper = new ObjectMapper();
    public static final String pattern = "%s@%s@%s";

    static {
        objectMapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
                .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
                .enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT)
                .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
                .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
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
        if (source == null || source.length == 0 || clazz == null) {
            return null;
        }

        return objectMapper.readValue(source, clazz);
    }

    public static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String format(long timestamp) {
        Date date = new Date(timestamp);
        return df.format(date);
    }

    public static String toHexString(Object obj) {
        try {
            if (obj instanceof byte[]) {
                return DigestUtils.md5Hex((byte[]) obj);
            } else if (obj instanceof String) {
                return DigestUtils.md5Hex((String) obj);
            } else if (obj instanceof InputStream) {
                return DigestUtils.md5Hex((InputStream) obj);
            } else {
                byte[] bytes = object2Byte(obj);
                return DigestUtils.md5Hex(bytes);
            }
        } catch (Throwable t) {
            throw new RStreamsException("object to HexString error, object=" + obj, t);
        }

    }


    public static byte[] long2Bytes(long time) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(time);
        return buffer.array();
    }

    public static long bytes2Long(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return 0;
        }

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.put(bytes);
        buffer.flip();//need flip
        return buffer.getLong();
    }

    public static byte[] watermarkKeyBytes(MessageQueue stateTopicMessageQueue, String watermarkPrefix) {
        if (stateTopicMessageQueue == null || StringUtils.isBlank(watermarkPrefix)) {
            throw new IllegalArgumentException();
        }

        String key = Utils.buildKey(watermarkPrefix,
                stateTopicMessageQueue.getBrokerName(),
                stateTopicMessageQueue.getTopic(),
                String.valueOf(stateTopicMessageQueue.getQueueId()));

        assert key != null;
        return key.getBytes(StandardCharsets.UTF_8);
    }
}
