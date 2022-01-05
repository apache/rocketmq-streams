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
package org.apache.rocketmq.streams.common.optimization;

import com.google.re2j.Matcher;
import com.google.re2j.Pattern;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * do regex match in bulk by re2
 *
 * @param <T> the type of associated info
 * @author arthur liang
 */
public class Re2Engine<T> implements IStreamRegex<T> {

    protected com.google.re2j.Pattern pattern;

    private Map<String, List<T>> expressionMap = new HashMap<>(128);

    private Map<String, String> nameMap = new HashMap<>(128);

    protected Map<String, String> unSupportMap = new HashMap<>(32);

    @Override public void addRegex(String regex, T context) {
        String groupName = "P" + nameMap.size();
        if (!expressionMap.containsKey(regex)) {
            expressionMap.put(regex, new ArrayList<>());
        }
        expressionMap.get(regex).add(context);
        if (!nameMap.containsKey(regex)) {
            nameMap.put(regex, groupName);
        }
    }

    @Override public void compile() {
        StringBuffer buffer = new StringBuffer();
        Iterator<Map.Entry<String, String>> iterator = nameMap.entrySet().iterator();
        com.google.re2j.Pattern testPattern;
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String expression = entry.getKey();
            String groupName = entry.getValue();
            try {
                testPattern = com.google.re2j.Pattern.compile(expression, com.google.re2j.Pattern.MULTILINE);
            } catch (Exception e) {
                iterator.remove();
                unSupportMap.put(groupName, expression);
                continue;
            }
            if (buffer.length() != 0) {
                buffer.append("|");
            }
            buffer.append("(?P<").append(groupName).append(">(").append(expression).append("))");
        }
        if (buffer.length() != 0) {
            pattern = com.google.re2j.Pattern.compile(buffer.toString(), com.google.re2j.Pattern.MULTILINE & Pattern.CASE_INSENSITIVE);
        }
    }

    @Override public boolean match(String content) {
        if (pattern == null || StringUtils.isBlank(content)) {
            return false;
        }
        Matcher matcher = pattern.matcher(content);
        if (matcher.find()) {
            return true;
        }
        if (!unSupportMap.isEmpty()) {
            return normalMatchBoolean(content);
        }
        return false;
    }

    private Set<T> normalMatchSet(String content) {
        Set<T> matchedSet = new HashSet<>();
        Iterator<Map.Entry<String, String>> iterator = unSupportMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String regex = entry.getValue();
            if (StringUtil.matchRegexCaseInsensitive(content, regex)) {
                matchedSet.addAll(expressionMap.get(regex));
            }
        }
        return matchedSet;
    }

    private boolean normalMatchBoolean(String content) {
        Iterator<Map.Entry<String, String>> iterator = unSupportMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            String regex = entry.getValue();
            if (StringUtil.matchRegexCaseInsensitive(content, regex)) {
                return true;
            }
        }
        return false;
    }

    @Override public Set<T> matchExpression(String content) {
        if (pattern == null || StringUtils.isBlank(content)) {
            return new HashSet<>();
        }
        Set<T> matchedSet = new HashSet<>();
        Matcher matcher = pattern.matcher(content);
        int index = 0;
        while (matcher.find()) {
            Iterator<Map.Entry<String, String>> iterator = nameMap.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                String groupName = entry.getValue();
                String expression = entry.getKey();
                if (matcher.group(groupName) != null) {
                    matchedSet.addAll(expressionMap.get(expression));
                    break;
                }
            }
        }
        if (!unSupportMap.isEmpty()) {
            Set<T> theNormalSet = normalMatchSet(content);
            matchedSet.addAll(theNormalSet);
        }
        return matchedSet;
    }

    @Override public int size() {
        return expressionMap.values().stream().mapToInt(list -> list.size()).sum();
    }
}
