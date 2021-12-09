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
package org.apache.rocketmq.streams.script.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.IgnoreMessage;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class FunctionUtils {
    private static final String FLAG_PREFIX = "constants_";//常量flag的前缀

    public static boolean isField(String fieldName) {
        return !isConstantOrNumber(fieldName);
    }

    public static boolean isConstantOrNumber(String fieldName) {
        if (isConstant(fieldName)) {
            return true;
        }
        if (isNumber(fieldName)) {
            return true;
        }
        return false;

    }

    public static boolean isConstant(String fieldName) {
        if (StringUtil.isEmpty(fieldName)) {
            return false;
        }
        if (fieldName.startsWith("'") && fieldName.endsWith("'")) {
            return true;
        }
        if (fieldName.startsWith("\"") && fieldName.endsWith("\"")) {
            return true;
        }
        return false;

    }

    private final static String LONG = "^[-\\+]?[\\d]*$";            // 整数
    private final static String DOUBLE = "^[-\\+]?[\\d]*[\\.]?[\\d]*$";

    public static boolean isNumberObject(Object object) {
        if (Long.class.isInstance(object)) {
            return true;
        } else if (Double.class.isInstance(object)) {
            return true;
        }
        return false;
    }

    public static boolean isNumber(String fieldName) {
        if (isLong(fieldName)) {
            return true;
        }
        return isDouble(fieldName);
    }


    public static boolean isLong(String fieldName) {
        if(StringUtil.isEmpty(fieldName)){
            return false;
        }
        boolean match = StringUtil.matchRegex(fieldName, LONG);
        return match;
    }

    public static boolean isDouble(String fieldName) {
        if(StringUtil.isEmpty(fieldName)){
            return false;
        }
        boolean match = StringUtil.matchRegex(fieldName, DOUBLE);
        return match;
    }

    public static boolean isBoolean(String fieldName) {
        if(StringUtil.isEmpty(fieldName)){
            return false;
        }
        String value = fieldName.toLowerCase();
        if ("true".equals(value) || "false".equals(value)) {
            return true;
        }
        return false;
    }

    public static Long getLong(String fieldName) {
        return Long.valueOf(fieldName);
    }

    public static Boolean getBoolean(String fieldName) {
        return Boolean.valueOf(fieldName);
    }

    public static Double getDouble(String fieldName) {
        return Double.valueOf(fieldName);
    }

    public static String getConstant(String fieldName) {
        if (StringUtil.isEmpty(fieldName)) {
            return fieldName;
        }
       // fieldName = fieldName.trim();
        if (fieldName.startsWith("'") && fieldName.endsWith("'")) {
            if (fieldName.equals("''")) {
                return "";
            }
            return fieldName.substring(1, fieldName.length() - 1);
        }
        return fieldName;
    }

    public static Object getValue(IMessage message, AbstractContext context, String fieldName) {
        Object value = getFiledValue(message, context, fieldName);
        if (value != null) {
            return value;
        }
        if (isConstant(fieldName)) {
            return getConstant(fieldName);

        } else if (isLong(fieldName)) {
            return getLong(fieldName);
        } else if (isDouble(fieldName)) {
            return getDouble(fieldName);
        } else if (isBoolean(fieldName)) {
            return getBoolean(fieldName);
        } else if("".equals(fieldName)){
            return "";
        }
        else {
            return null;
        }
    }

    public static Object getFiledValue(IMessage message, AbstractContext context, String fieldName) {
        if (context == null || message == null) {
            return fieldName;
        }
        if (IgnoreMessage.class.isInstance(message)) {
            return fieldName;
        }

        Object value = message.getMessageBody().get(fieldName);
        return value;
    }
    private static DateDataType dateDataType=new DateDataType();
    public static String getValueString(IMessage message, AbstractContext context, String fieldName) {
        Object value = getValue(message, context, fieldName);
        if (value == null) {
            return null;
        }
        if (String.class.isInstance(value)) {
            return (String)value;
        }
        if(dateDataType.matchClass(value.getClass())){
            return dateDataType.toDataJson(dateDataType.convert(value));
        }
        return value.toString();
    }

    public static Long getValueLong(IMessage message, AbstractContext context, String fieldName) {
        return getLong(getValueString(message, context, fieldName));
    }

    public static Double getValueDouble(IMessage message, AbstractContext context, String fieldName) {
        return getDouble(getValueString(message, context, fieldName));
    }

    public static Integer getValueInteger(IMessage message, AbstractContext context, String fieldName) {
        return getLong(getValueString(message, context, fieldName)).intValue();
    }

    /**
     * 对带有常量的脚本预处理，把常量替换成flag，flag和常量的关系放在map里
     *
     * @param scriptOrExpression 待处理的脚本
     * @param cache              存放处理后的flag和原常量
     * @return
     */
    public static String doPreConstants(String scriptOrExpression, Map<String, String> cache) {
        String tmp = doPreConstants(scriptOrExpression, cache, "'");
        tmp = doPreConstants(tmp, cache, "\"");
        return tmp;
    }

    /**
     * 把进行过常量处理的还原回原来的常量形式
     *
     * @param hasPreScriptOrExpression
     * @param cache
     * @return
     */
    public static String doRecoverConstants(String hasPreScriptOrExpression, Map<String, String> cache) {
        Iterator<Map.Entry<String, String>> it = cache.entrySet().iterator();
        String tmp = hasPreScriptOrExpression;
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            String flag = entry.getKey();
            String value = entry.getValue();
            tmp = tmp.replace(flag, "'" + value + "'");
        }
        return tmp;
    }

    /**
     * 对带有常量的脚本预处理，把常量替换成flag，flag和常量的关系放在map里
     *
     * @param scriptOrExpression 待处理的脚本
     * @param cache              存放处理后的flag和原常量
     * @return
     */
    private static String doPreConstants(String scriptOrExpression, Map<String, String> cache, String constantsSign) {
        boolean openConstant = false;
        String reslut = scriptOrExpression;
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < scriptOrExpression.length(); i++) {
            String word = scriptOrExpression.substring(i, i + 1);
            if (constantsSign.equals(word) && openConstant) {
                openConstant = false;
                int flag = cache.size() + 1;
                String constant = value.toString();
                cache.put(FLAG_PREFIX + flag, constant);
                reslut = reslut.replace(constantsSign + constant + constantsSign, FLAG_PREFIX + flag);
                value = new StringBuilder();
            } else if (openConstant) {
                value.append(word);
            } else if (constantsSign.equals(word) && openConstant == false) {
                openConstant = true;
            }
        }
        return reslut;
    }

    public static void main(String[] args) {
        Map<String, String> cache = new HashMap<>();
        String tmp = doPreConstants("abc,'dfddf',\"dfdfd\"", cache);
        System.out.println(tmp);
        System.out.println(doRecoverConstants(tmp, cache));

        System.out.println(StringUtil.matchRegex("",LONG));
    }
}
