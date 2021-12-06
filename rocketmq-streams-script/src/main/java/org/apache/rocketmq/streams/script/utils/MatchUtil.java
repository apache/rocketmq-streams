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

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class MatchUtil {

    private static Pattern pattern = Pattern.compile("^[-\\+]?[\\d]*$");

    /**
     * 判断是否所有的字段都符合预期的值。否则返回false
     *
     * @param object        bean
     * @param fieldConditon
     * @return
     */
    public static boolean matchCondition(Object object, Map<String, List<String>> fieldConditon,
                                         boolean isAndRelation) {
        Iterator<Map.Entry<String, List<String>>> it = fieldConditon.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, List<String>> entry = it.next();
            String fieldName = entry.getKey();
            List<String> list = entry.getValue();
            Object fieldValue = ReflectUtil.getBeanFieldValue(object, fieldName);
            if (fieldValue == null) {
                return false;
            }
            String value = fieldValue.toString();
            boolean isMatch = StringMatchUtil.matchRegex(value, list);
            if (isAndRelation && !isMatch) {
                return false;
            }
            if (!isAndRelation && isMatch) {
                return true;
            }
        }
        if (isAndRelation) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isNumber(String str) {
        if (StringUtil.isEmpty(str)) {
            return false;
        }
        return pattern.matcher(str).matches();
    }

}
