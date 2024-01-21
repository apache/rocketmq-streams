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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class LogParserUtil {
    private static Set signs = new HashSet();

    static {
        signs.add(",");
        signs.add(" ");
        signs.add("|");
        signs.add("=");
    }

    public static String parse(String log, String... spiltSigns) {
        Map<String, String> flags = new HashMap<>();
        PreCal preCal = preCal(log);
        if (preCal.needContansts) {
            log = parseContants(log, flags);
        }
        if (preCal.needBrackets) {
            log = parseBrackets(log, flags);
        }

        log = parseDate(log, flags);
        log = parseContantsBySign(log, "|", "constants", new IntegerValue(), flags);
        //        Iterator<Map.Entry<String, String>> it = flags.entrySet().iterator();
        //        while (it.hasNext()){
        //            System.out.println(it.next().getValue());
        //        }
        //        int index=0;
        //        while (index<spiltSigns.length){
        //            String sign=spiltSigns[index];
        //            String[] values=log.split(sign);
        //            index=index+1;
        //            for(String value:values){
        //                for(int i=index;i<spiltSigns.length;i++){
        //                    if(value.indexOf(spiltSigns[i])!=-1){
        //
        //                    }
        //                }
        //            }
        //       }
        return log;
    }

    private static PreCal preCal(String log) {
        PreCal preCal = new PreCal();
        for (int i = 0; i < log.length() - 1; i++) {
            String word = log.substring(i, i + 1);
            if (word.equals("'") || word.equals("\"")) {
                preCal.needContansts = true;
            }
            if (word.equals("(") || word.equals("[") || word.equals("{")) {
                preCal.needBrackets = true;
            }
        }
        return preCal;
    }

    public static String parseContantsBySign(String log, String sign, String prefix) {
        return parseContantsBySign(log, sign, prefix, new IntegerValue(), new HashMap<>());
    }

    public static String parseContantsBySign(String log, String sign, String prefix, IntegerValue index, Map<String, String> flags) {

        String tmp = log;
        int startIndex = log.indexOf(sign);
        if (startIndex == -1) {
            return log;
        }
        tmp = log.substring(startIndex + 1);
        int endIndex = tmp.indexOf(sign);
        if (endIndex == -1) {
            String word = sign + tmp;
            String key = prefix + index.get();
            flags.put(key, word);
            index.incr();
            log = log.replace(word, key);
            return log;
        }
        endIndex += startIndex + 1;
        String word = log.substring(startIndex, endIndex + 1);
        String key = prefix + index.get();
        flags.put(key, word);
        index.incr();
        word = word.substring(0, word.length() - 1);
        log = log.replace(word, key);
        return parseContantsBySign(log, sign, prefix, index, flags);
    }

    //    public static String parseExpression(String log){
    //
    //    }

    public static String parseContants(String log) {
        return parseContants(log, new HashMap<>());
    }

    public static String parseBrackets(String log) {
        return parseBrackets(log, new HashMap<>());
    }

    public static String parseDate(String log, Map<String, String> flag) {
        return log;
    }

    /**
     * 解析常量，用单引号和双引号标注的，一般认为是常量，不需要分割
     *
     * @param log
     * @param flag
     * @return
     */
    public static String parseContants(String log, Map<String, String> flag) {
        return ContantsUtil.doConstantReplace(log, flag, 1);
    }

    /**
     * 解析用括号（大，中，小）分割的，一般认为是个整体，不需要分割
     *
     * @param log
     * @param flag
     * @return
     */
    public static String parseBrackets(String log, Map<String, String> flag) {
        IntegerValue integerValue = new IntegerValue();
        String tmp = parseBrackets(log, "bracket", "[", "]", integerValue, flag);
        tmp = parseBrackets(tmp, "bracket", "{", "}", integerValue, flag);
        tmp = parseBrackets(tmp, "bracket", "(", ")", integerValue, flag);
        return tmp;
    }

    public static String parseBrackets(String log, Map<String, String> flag, String... signs) {
        IntegerValue integerValue = new IntegerValue();
        String tmp = log;
        for (String sign : signs) {
            if ("[".equals(sign)) {
                tmp = parseBrackets(tmp, "bracket", "[", "]", integerValue, flag);
            } else if ("{".equals(sign)) {
                tmp = parseBrackets(tmp, "bracket", "{", "}", integerValue, flag);
            } else if ("(".equals(sign)) {
                tmp = parseBrackets(tmp, "bracket", "(", ")", integerValue, flag);
            } else if ("<".equals(sign)) {
                tmp = parseBrackets(tmp, "bracket", "<", ">", integerValue, flag);
            }
        }
        return tmp;
    }

    /**
     * 把括号中的值提取出来
     *
     * @param log          原始日志
     * @param prefix       变量替换的前缀
     * @param startBracket 开括号
     * @param endBracket   闭括号
     * @param index        变量替换的下标
     * @param flag         存放变量和原始值的映射
     * @return
     */
    protected static String parseBrackets(String log, String prefix, String startBracket, String endBracket, IntegerValue index, Map<String, String> flag) {
        int startIndex = log.indexOf(startBracket);
        if (startIndex == -1) {
            return log;
        }
        int endIndex = log.indexOf(endBracket);
        if (endIndex == -1) {
            return log;
        }
        String word = log.substring(startIndex, endIndex + 1);
        String key = prefix + index.get();
        flag.put(key, word);
        index.incr();
        log = log.replace(word, key);
        return parseBrackets(log, prefix, startBracket, endBracket, index, flag);
    }

    //act=alert alertcreatetime=19 November 2019 15:55:01 dst=10.190.173.201 dpt=80 src=117.24.13.15 spt=28457 proto=TCP cat=Alert cs1=servergroup74 cs1Label=ServerGroup cs2=servergroup74_HTTP cs2Label=ServiceName cs3=Default Web Application cs3Label=ApplicationName deviceExternalId=TL_Imperva_WAF_01 httpHost=61.146.73.160 url=61.146.73.160:87 alertname=custom-policy-violation alertDescription=custom-policy-violation eventId=222068197740949112 responseCode="
    public static Map<String, String> parseExpression(String log, String sign) {
        log += " ";
        String[] values = log.split("=");
        Map<String, String> result = new HashMap<>();
        String key = values[0];
        int length = values.length;
        for (int i = 1; i < values.length - 1; i++) {
            String tmp = values[i];
            String[] tmps = tmp.split(sign);
            String nextKey = tmps[tmps.length - 1];
            StringBuilder sb = new StringBuilder();
            for (int j = 0; j < tmps.length - 1; j++) {
                if (j > 0) {
                    sb.append(sign);
                }
                sb.append(tmps[j]);
            }
            String value = sb.toString();
            result.put(key, value);
            key = nextKey;
        }
        result.put(key, values[values.length - 1].trim());
        return result;
    }

    private static class PreCal {
        boolean needContansts = false;
        boolean needBrackets = false;
    }

    protected static class IntegerValue {
        private int i = 1;

        public int incr() {
            i++;
            return i;
        }

        public int get() {
            return i;
        }
    }
}
