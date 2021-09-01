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
package org.apache.rocketmq.streams.window.operator.join;

import com.alibaba.fastjson.JSONObject;
import java.security.MessageDigest;
import java.util.List;

public interface Operator {

    public static String generateKey(JSONObject messageBody, String joinLabel, List<String> leftJoinFieldNames,
        List<String> rightJoinFieldNames) {
        StringBuffer buffer = new StringBuffer();
        if ("left".equalsIgnoreCase(joinLabel)) {
            for (String field : leftJoinFieldNames) {
                String value = messageBody.getString(field);
                buffer.append(value).append("_");
            }
        } else {
            for (String field : rightJoinFieldNames) {
                String[] rightFields = field.split(".");
                if (rightFields.length > 1) {
                    field = rightFields[1];
                }
                String value = messageBody.getString(field);
                buffer.append(value).append("_");
            }
        }

        buffer.charAt(buffer.length() - 1);

        return MD5(buffer.toString());
    }

    public static String MD5(String s) {
        char hexDigits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

        try {
            byte[] btInput = s.getBytes();
            // 获得MD5摘要算法的 MessageDigest 对象
            MessageDigest mdInst = MessageDigest.getInstance("MD5");
            // 使用指定的字节更新摘要
            mdInst.update(btInput);
            // 获得密文
            byte[] md = mdInst.digest();
            // 把密文转换成十六进制的字符串形式
            int j = md.length;
            char str[] = new char[j * 2];
            int k = 0;
            for (int i = 0; i < j; i++) {
                byte byte0 = md[i];
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];
                str[k++] = hexDigits[byte0 & 0xf];
            }
            return new String(str);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e.getMessage(), e);
        }
    }

}