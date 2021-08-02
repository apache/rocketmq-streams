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

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StringMatchUtil {
    /**
     * 正则匹配，只要list有一个匹配返回true
     *
     * @param content
     * @param list
     * @return
     */
    public static boolean matchRegex(String content, List<String> list) {
        for (String configure : list) {
            if (matchRegex(content, configure)) {
                return true;
            }
        }
        return false;
    }

    /**
     * content为字符串，configure为pattern
     *
     * @param content
     * @param configure
     * @return
     */
    public static boolean matchRegex(String content, String configure) {
        Pattern pattern = Pattern.compile(configure);
        Matcher matcher = pattern.matcher(content);
        if (matcher.find()) {
            return true;
        }
        return false;
    }

    /**
     * 尾号匹配
     *
     * @param content
     * @param list
     * @return
     */
    public static boolean matchRear(String content, List<String> list) {
        for (String configure : list) {
            if (content.endsWith(configure)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 全匹配
     *
     * @param content
     * @param list
     * @return
     */
    public static boolean matchCompletely(String content, List<String> list) {
        for (String configure : list) {
            if (content.equals(configure)) {
                return true;
            }
        }
        return false;
    }
}
