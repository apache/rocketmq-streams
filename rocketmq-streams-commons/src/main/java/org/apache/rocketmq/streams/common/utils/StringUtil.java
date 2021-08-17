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

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.optimization.OptimizationRegex;

public final class StringUtil {

    /**
     * 是否开启 用于快速优化，抽取正则里面的词，能够先匹配，目的是快速失效 的开关 默认为false
     */
    private static boolean regex_fast_switch = false;

    static {
        // dipper.properties 配置文件中 获取
        String propertyFlag = ComponentCreator.getProperties().getProperty("regex.fast.switch");
        if ("true".equals(propertyFlag)) {
            regex_fast_switch = true;
        }
    }

    public static boolean isEmpty(String string) {
        return string == null || "".equals(string.trim());
    }

    public static boolean isNotEmpty(String string) {
        return string != null && !"".equals(string.trim());
    }

    public static boolean hasLength(CharSequence str) {
        return str != null && str.length() > 0;
    }

    public static String stripToNull(String string) {
        if (string == null || string.isEmpty()) {
            return null;
        }
        return string;
    }

    public static String getUUID() {
        //        String uuid = UUID.randomUUID().toString().replace("-", "");
        String uuid = "";
        String str = String.valueOf(System.currentTimeMillis());
        List list = new ArrayList();
        //将时间戳放入到List中
        for (Character s : str.toCharArray()) {
            list.add(s.toString());
        }
        //随机打乱
        Collections.shuffle(list);
        //拼接字符串，并添加2(自定义)位随机数
        String timeStr = String.join("", list) + randomNumber(2);
        uuid += timeStr;
        return uuid;
    }

    /**
     * 生成指定长度的一个数字字符串
     *
     * @param num
     * @return
     */
    public static String randomNumber(int num) {
        if (num < 1) {
            num = 1;
        }
        Random random = new Random();
        StringBuilder str = new StringBuilder();
        for (int i = 0; i < num; i++) {
            str.append(random.nextInt(10));
        }
        return str.toString();
    }

    public static boolean equals(String str1, String str2) {
        return str1 == null ? str2 == null : str1.equals(str2);
    }

    public static String toString(String[] array) {
        if (array == null) {
            return "";
        }
        if (array.length == 1) {
            return array[0];
        }
        StringBuilder buf = new StringBuilder(64);
        for (String item : array) {
            buf.append(item).append(",");
        }
        if (buf.length() > 0) {
            buf.deleteCharAt(buf.length() - 1);
        }
        return buf.toString();
    }

    public static String left(String str, int len) {
        if (str == null || len < 0) {
            return null;
        }

        if (str.length() <= len) {
            return str;
        } else {
            return str.substring(0, len);
        }
    }

    public static String concatStringBySplitSign(String sign, List<String> values) {
        if (values == null) {
            return null;
        }
        String[] paras = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
            paras[i] = values.get(i);
        }
        return concatStringBySplitSign(sign, paras);
    }

    public static String concatStringBySplitSign(String sign, String... values) {
        return concatStringBySplitSign(sign, null, null, values);
    }

    public static String concatStringBySplitSign(String sign, Integer fromIndex, Integer toIndex, String... values) {
        if (values == null) {
            return null;
        }
        int fIndex = fromIndex == null ? 0 : fromIndex;
        int tIndex = toIndex == null ? values.length : toIndex;
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (int i = fIndex; i < tIndex; i++) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(sign);
            }
            if (values[i] != null) {
                stringBuilder.append(values[i]);
            }
        }
        return stringBuilder.toString();
    }

    /**
     * content为字符串，configure为pattern
     *
     * @param content
     * @param patternStr
     * @return
     */
    public static boolean matchRegex(String content, String patternStr) {
        if (matchQuickWord(content, patternStr) == false) {
            return false;
        }

        Matcher matcher = createMatcher(content, patternStr, false, -1);

        if (matcher.find()) {
            return true;
        }
        return false;
    }

    /**
     * 忽略大小写 content为字符串，configure为pattern
     *
     * @param content
     * @param patternStr
     * @return
     */
    public static boolean matchRegexCaseInsensitive(String content, String patternStr) {

        if (matchQuickWord(content, patternStr) == false) {
            return false;
        }
        Matcher matcher = createMatcher(content, patternStr, true, -1);
        if (matcher.find()) {
            return true;
        }
        return false;
    }

    /**
     * 从正则里面抽取词，如鬼词预先匹配成功才会继续执行正则
     *
     * @param content
     * @param patternStr
     * @return
     */
    public static boolean matchQuickWord(String content, String patternStr) {
        // 当正则表达式快速开关为开启时，不进行正则快速校验
        if (!regex_fast_switch) {
            return true;
        }
        OptimizationRegex optimizationRegex = OptimizationRegex.optimizationRegexMap.get(patternStr);
        if (optimizationRegex == null) {
            optimizationRegex = new OptimizationRegex(patternStr);
            OptimizationRegex.optimizationRegexMap.put(patternStr, optimizationRegex);
        }
        // 当启用优化正则表达式的开关开启后，把前置过滤的词，放入cache中。

        return optimizationRegex.quickMatch(content);
    }

    /**
     * content为字符串，configure为pattern
     *
     * @param content
     * @param patternStr
     * @return
     */
    public static String groupRegex(String content, String patternStr) {
        return groupRegex(content, patternStr, 1);
    }

    /**
     * content为字符串，configure为pattern
     *
     * @param content
     * @param patternStr
     * @return
     */
    public static String groupRegex(String content, String patternStr, int gourp) {
        Matcher matcher = createMatcher(content, patternStr, false, -1);
        if (matchQuickWord(content, patternStr) == false) {
            return null;
        }
        if (matcher.find()) {
            return matcher.group(gourp);
        }
        return null;
    }

    public static boolean isSameString(String s1, String s2) {
        if (s1 == null && s2 == null) {
            return true;
        } else if (s1 != null && s2 != null) {
            return s1.equals(s2);
        }
        return false;
    }

    /**
     * 根据要匹配的字符串，匹配模式字符串，是否忽略大小写和超时时间创建matcher
     *
     * @param content 待匹配的字符串
     * @param patternStr 匹配模式
     * @param caseInsensitive 是否忽略大小写
     * @param timeout 超时时间
     * @return
     */
    private static Map<String, Pattern> pattern2MatcherForCaseInsensitive = new HashMap<String, Pattern>();
    private static Map<String, Pattern> pattern2Matcher = new HashMap<String, Pattern>();

    private static Matcher createMatcher(String content, String patternStr, boolean caseInsensitive, long timeout) {
        Map<String, Pattern> map = null;
        if (caseInsensitive) {
            map = pattern2MatcherForCaseInsensitive;
        } else {
            map = pattern2Matcher;
        }
        Pattern pattern = map.get(patternStr);
        if (pattern != null) {
            return pattern.matcher(content);
        }
        synchronized (StringUtil.class) {
            pattern = map.get(patternStr);
            if (pattern != null) {
                return pattern.matcher(content);
            }
            if (caseInsensitive) {
                pattern = Pattern.compile(patternStr, Pattern.CASE_INSENSITIVE);
            } else {
                pattern = Pattern.compile(patternStr);
            }
            map.put(patternStr, pattern);

            Matcher matcher = pattern.matcher(content);
            return matcher;
        }

    }

    /**
     * 把一个字符串先做md5，再转化成base64字符串
     *
     * @param key
     * @return
     */
    public static String createMD5Str(String key) {
        byte[] hashCodes = AESUtil.stringToMD5(key);
        String result = null;
        try {
            result = Base64Utils.encode(hashCodes);
        } catch (Exception e) {
            e.printStackTrace();
            return key;
        }
        return result;
    }

    /**
     * 计算字符串中出现某个符号的个数
     *
     * @param line
     * @param sign
     * @return
     */
    public static int cacuSignCount(String line, String sign) {
        if (StringUtil.isEmpty(line)) {
            return 0;
        }
        int count = 0;
        int index = line.indexOf(sign);
        while (index > -1 && index <= line.length()) {
            count++;
            int tmp = index + 1;
            index = line.indexOf(sign, tmp);
        }
        return count;
    }

    public static String defaultString(String propertyValue, String defalut) {
        if (StringUtil.isEmpty(propertyValue)) {
            return defalut;
        }
        return propertyValue;
    }

    private static String hexString = "0123456789abcdef";

    /*
     * 将16进制数字解码成字符串,适用于所有字符（包括中文）
     */
    public static String decode16(String bytes) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(bytes.length() / 2);
        // 将每2位16进制整数组装成一个字节
        for (int i = 0; i < bytes.length(); i += 2) {
            // 防止奇数位的字符串出现
            if (i >= bytes.length() - 1) {
                continue;
            }
            baos.write((hexString.indexOf(bytes.charAt(i)) << 4 | hexString.indexOf(bytes.charAt(i + 1))));
        }

        return new String(baos.toByteArray());
    }
}
