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
package org.apache.rocketmq.streams.filter.utils;

import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.exception.RegexTimeoutException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexUtil {

    // private static final long MAX_TIMEOUT = 1000;// 如果没有设置超时时间，最大的超时时间默认为1s

    private static class InterruptibleCharSequence implements CharSequence {

        private CharSequence inner;
        private long timeout;
        private boolean isStart = false;
        private long startTime = -1;

        public InterruptibleCharSequence(CharSequence inner, long timeout) {
            super();
            this.inner = inner;
            this.timeout = timeout;
        }

        @Override
        public int length() {
            return inner.length();
        }

        @Override
        public char charAt(int index) {
            if (!isStart) {
                isStart = true;
                startTime = System.currentTimeMillis();
            }
            if (System.currentTimeMillis() - startTime > timeout && timeout > 0) {
                throw new RegexTimeoutException("正则执行超时：" + inner.toString());
            }
            return inner.charAt(index);

        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return new InterruptibleCharSequence(inner.subSequence(start, end), timeout);
        }

        @Override
        public String toString() {
            return inner.toString();
        }
    }

    /**
     * content为字符串，configure为pattern
     *
     * @param content
     * @param patternStr
     * @return 如果超时会抛出超时错误
     */
    public static boolean matchRegex(String content, String patternStr, boolean caseInsensitive, long timeout) {
        try {
            if (content == null) {
                return false;
            }
            // 去掉换行符\n
            if (content.contains("\n")) {
                content = content.replaceAll("\n", "");
            }
            // 防止var变量值过长，严重影响正则执行性能
            if (content.length() > 1000) {
                content = content.substring(0, 1000);
            }
            if (caseInsensitive) {
                return StringUtil.matchRegexCaseInsensitive(content, patternStr);
            } else {
                return StringUtil.matchRegex(content, patternStr);
            }
        } catch (RegexTimeoutException e) {
            throw new RegexTimeoutException(patternStr, content, timeout);
        }

    }

    /**
     * content为字符串，configure为pattern
     *
     * @param content
     * @param patternStr
     * @return
     */
    public static String groupRegex(String content, String patternStr, boolean caseInsensitive, long timeout) {
        try {
            // 去掉换行符\n
            if (content.contains("\n")) {
                content = content.replaceAll("\n", "");
            }
            Matcher matcher = createMatcher(content, patternStr, caseInsensitive, timeout);
            if (matcher.find()) {
                return matcher.group(1);
            }
            return null;
        } catch (RegexTimeoutException e) {
            throw new RegexTimeoutException(patternStr, content, timeout);
        }
    }

    private static final String SPLIT_STR =
        "\\d|\\w|\\s|\\|\\$|\\(|\\)|\\*|\\+|\\.|\\[|\\]|\\?|\\^|\\{|\\}|\\|";

    public static List<String> compilePattern(String regex) {
        List<String> strs = new ArrayList<>();
        // List<String> keywords = new ArrayList<String>();
        if (regex.indexOf("|") != -1) {
            return new ArrayList<>();
        }
        String[] str = regex.split(SPLIT_STR);
        for (String temp : str) {
            if (temp.length() >= 3 && !temp.contains(",")) {
                strs.add(temp);
            }
        }
        return strs;
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
    private static Map<String, Pattern> pattern2MatcherForCaseInsensitive = new HashMap<>();
    private static Map<String, Pattern> pattern2Matcher = new HashMap<>();

    private static Matcher createMatcher(String content, String patternStr, boolean caseInsensitive, long timeout) {
        Map<String, Pattern> map = null;
        if (caseInsensitive) {
            map = pattern2MatcherForCaseInsensitive;
        } else {
            map = pattern2Matcher;
        }

        Pattern pattern = map.get(patternStr);
        if (pattern != null) {
            // return pattern.matcher(new InterruptibleCharSequence(content, timeout));
            return pattern.matcher(content);
        }
        synchronized (RegexUtil.class) {
            pattern = map.get(patternStr);
            if (pattern != null) {
                // return pattern.matcher(new InterruptibleCharSequence(content, timeout));
                return pattern.matcher(content);
            }
            if (caseInsensitive) {
                try {
                    pattern = Pattern.compile(patternStr, Pattern.CASE_INSENSITIVE);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            } else {
                pattern = Pattern.compile(patternStr);
            }
            map.put(patternStr, pattern);
            // Matcher matcher = pattern.matcher(new InterruptibleCharSequence(content, timeout));
            Matcher matcher = pattern.matcher(content);
            return matcher;
        }

    }

    // private static final String[] regexSign = { "\\d", "\\w", "\\s", "\\", "$", "(", ")", "*", "+", ".", "[", "]",
    // "?",
    // "^", "{", "}", "|" };

    public static void main(String args[]) {
        // String content = "REG ADD \"HKLM\\SOFTWARE\\Microsoft\\Windows NT\\CurrentVersion\\Image File Execution
        // Options\\sethc.exe\" /v Debugger /t REG_SZ /d \"C:\\windows\\system32\\cmd.exe\"";
        String patternStr = "python\\s+-c\\s+('')?import\\s+socket.*socket\\.socket\\(socket\\.af_inet.*\\.connect\\(.*subprocess\\.call";

        String message = "/bin/sh -c /usr/bin/w";

        System.out.println("@@@" + RegexUtil.matchRegex(message, patternStr, true, -1));

    }

}
