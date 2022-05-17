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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ContantsUtil {
    private static final List<String> CONSTANTS_SIGNS = new ArrayList<>();//对于特殊字符优先处理，里面存储需要特殊处理的字符
    private static final Map<String, String> CONSTANTS_SIGNS_REPLACE = new HashMap<>();// 特殊字符和替换字符的映射
    private static final Map<String, String> CONSTANTS_REPLACE_SIGNS = new HashMap<>();//替换字符和特殊字符的映射

    static {
        CONSTANTS_SIGNS.add("\\\\");
        CONSTANTS_SIGNS.add("\\\"");
        CONSTANTS_SIGNS.add("\"");
        CONSTANTS_SIGNS.add("\\'");
        CONSTANTS_SIGNS.add("''");
        CONSTANTS_SIGNS.add("#######");

        CONSTANTS_SIGNS_REPLACE.put("\\\\", "%%%%%");
        CONSTANTS_SIGNS_REPLACE.put("\\'", "^^^^");
        CONSTANTS_SIGNS_REPLACE.put("\\\"", "~~~~~");
        CONSTANTS_SIGNS_REPLACE.put("''", "*****");
        CONSTANTS_SIGNS_REPLACE.put("\"", "&@--@&");
        CONSTANTS_SIGNS_REPLACE.put("#######", "#######");


        CONSTANTS_REPLACE_SIGNS.put("%%%%%", "\\\\");
        CONSTANTS_REPLACE_SIGNS.put("&@--@&", "\"");
        CONSTANTS_REPLACE_SIGNS.put("^^^^", "'");
        CONSTANTS_REPLACE_SIGNS.put("~~~~~", "\\\"");
        CONSTANTS_REPLACE_SIGNS.put("*****", "''");
        CONSTANTS_REPLACE_SIGNS.put("#######", "'");
    }

    /**
     * 替换特殊字符为替换字符串
     *
     * @param str
     * @return
     */
    public static String replaceSpeciaSign(String str) {
        for (String sign : CONSTANTS_SIGNS) {
            str = str.replace(sign, CONSTANTS_SIGNS_REPLACE.get(sign));
        }
        return str;
    }

    public static String restoreSpecialSign(String str) {
        for (String sign : CONSTANTS_REPLACE_SIGNS.keySet()) {
            str = str.replace(sign, CONSTANTS_REPLACE_SIGNS.get(sign));
        }
        return str;
    }

    public static String restoreConstantAndNotRestoreSpeical(String scriptStr, Map<String, String> flag2ExpressionStr) {
        return restore(scriptStr, flag2ExpressionStr, false);
    }

    public static String restore(String scriptStr, Map<String, String> flag2ExpressionStr) {
        return restore(scriptStr, flag2ExpressionStr, true);
    }

    private static String restore(String scriptStr, Map<String, String> flag2ExpressionStr, boolean isRestoreSpecial) {
        if (flag2ExpressionStr == null) {
            return scriptStr;
        }
        Iterator<Map.Entry<String, String>> it = flag2ExpressionStr.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> entry = it.next();
            String key = entry.getKey();
            String value = entry.getValue();
            scriptStr = scriptStr.replace(key, value);
        }
        if (!isRestoreSpecial) {
            return scriptStr;
        }
        return restoreSpecialSign(scriptStr);
    }

    /**
     * 判断是否是常量的开始
     *
     * @param expressionStr
     * @param i
     * @return
     */
    public static boolean isContantsStart(String expressionStr, int i) {
        //        List<String> flags=new ArrayList<>();
        //        flags.add("('");
        //        flags.add("='");
        //        flags.add(",'");
        //        flags.add("{'");
        return isContantsStart(expressionStr, i, null);

    }

    /**
     * 判断是否是常量的开始
     *
     * @param expressionStr
     * @param i
     * @return
     */

    public static boolean isContantsStart(String expressionStr, int i, List<String> flags) {
        boolean success = isContantsStart(expressionStr, "'", i, flags);
        if (success) {
            return true;
        }
        success = isContantsStart(expressionStr, "\"", i, flags);
        return success;
    }

    /**
     * 判断是否是常量的开始
     *
     * @param expressionStr
     * @param i
     * @return
     */

    protected static boolean isContantsStart(String expressionStr, String sign, int i, List<String> flags) {
        if (i < 1) {
            return false;
        }
        String word = expressionStr.substring(i - 1, i);
        if (!sign.equals(word)) {
            return false;
        }

        if (i > 1) {
            String tmp = expressionStr.substring(i - 2, i);
            if ((sign + "" + sign).equals(tmp)) {
                return false;
            }
        }
        if (i < expressionStr.length() - 1 && i > 0) {
            String tmp = expressionStr.substring(i - 1, i + 1);
            if ((sign + "" + sign).equals(tmp)) {
                return false;
            }
        }
        return true;
    }

    public static String doConstantReplace(String expressionStr, Map<String, String> flag2ExpressionStr, int flag,
                                           List<String> startFlags, List<String> endFlags) {
        if (expressionStr.indexOf("'") == -1) {
            return expressionStr;
        }
        return doConstantReplace(expressionStr, flag2ExpressionStr, flag, startFlags, endFlags, true);
    }

    public static String doConstantReplace(String expressionStr, Map<String, String> flag2ExpressionStr, int flag,
                                           List<String> startFlags, List<String> endFlags, boolean needReplaceSpecialSign) {

        if (needReplaceSpecialSign) {
            expressionStr = replaceSpeciaSign(expressionStr);
        }
        int startCount = 0;
        int gap = 0;
        String result = expressionStr;
        StringBuffer sb = new StringBuffer();
        int index = -1;
        for (int i = 1; i <= expressionStr.length(); i++, gap++) {
            String word = expressionStr.substring(i - 1, i);
            if (startCount == 0) {
                boolean isContantsStart = false;
                if (startFlags != null && startFlags.size() > 0) {
                    isContantsStart = isContantsStart(expressionStr, i, startFlags);
                } else {
                    isContantsStart = isContantsStart(expressionStr, i);
                }
                if (isContantsStart) {
                    startCount++;
                    sb.append(word);
                    continue;
                }
            }
            if (startCount > 0) {
                sb.append(word);
            }
            boolean isContantsEnd = false;
            if (endFlags != null && endFlags.size() > 0) {
                isContantsEnd = isContantsEnd(expressionStr, i, endFlags);
            } else {
                isContantsEnd = isContantsEnd(expressionStr, i);
            }
            if (isContantsEnd) {
                startCount--;
                if (startCount == 0) {
                    String constants = sb.toString();
                    sb = new StringBuffer();
                    constants = constants.trim();
                    if (!constants.endsWith("'") && !constants.endsWith("\"")) {
                        constants = constants.substring(0, constants.length() - 1);
                    }
                    String tmpFlag = createFlagKey(flag);
                    flag2ExpressionStr.put(tmpFlag, constants);
                    if (index == -1) {
                        index = i;
                    } else {
                        index = index + gap;
                    }
                    gap = 0;
                    result = result.substring(0, index).replace(constants, tmpFlag) + result.substring(index);
                    index = index - constants.length() + tmpFlag.length();
                    flag++;
                }
            }

        }
        return result;
    }

    public static String doConstantReplace(String expressionStr, Map<String, String> flag2ExpressionStr, int flag) {
        if (expressionStr.startsWith("if('") && expressionStr.endsWith("')")) {
            int startIndex = 4;
            int endIndex = expressionStr.length() - 2;
            String constant = expressionStr.substring(startIndex - 1, endIndex + 1);
            String tmpFlag = createFlagKey(flag);
            flag2ExpressionStr.put(tmpFlag, constant);
            expressionStr = expressionStr.replace(constant, tmpFlag);
        }
        return doConstantReplace(expressionStr, flag2ExpressionStr, flag, null, null);
    }

    public static String createConsKey(int flag) {
        String tmp = flag + "";
        if (flag < 10) {
            tmp = "00" + tmp;
        } else if (flag < 100) {
            tmp = "0" + tmp;
        }
        return "constants_para_" + tmp;
    }

    public static String createFlagKey(int flag) {
        String tmp = flag + "";
        if (flag < 10) {
            tmp = "00" + tmp;
        } else if (flag < 100) {
            tmp = "0" + tmp;
        }
        return "constants_" + tmp;
    }

    /**
     * 判断是否是常量的开始
     *
     * @param expressionStr
     * @param i
     * @return
     */
    public static boolean isContantsEnd(String expressionStr, int i) {
        List<String> flags = new ArrayList<>();
        //        flags.add("')");
        //        flags.add("',");
        //        flags.add("';");
        //        flags.add("'}");
        flags.add("''");
        return isContantsEnd(expressionStr, i, flags);
    }

    /**
     * 判断是否是常量的开始
     *
     * @param expressionStr
     * @param i
     * @return
     */
    public static boolean isContantsEnd(String expressionStr, int i, List<String> flags) {
        boolean success = isContantsEnd(expressionStr, "'", i, flags);
        if (success) {
            return true;
        }
        success = isContantsEnd(expressionStr, "\"", i, flags);
        return success;
    }

    /**
     * 判断是否是常量的开始
     *
     * @param expressionStr
     * @param i
     * @return
     */
    protected static boolean isContantsEnd(String expressionStr, String sign, int i, List<String> flags) {
        String word = expressionStr.substring(i - 1, i);
        if (!sign.equals(word)) {
            return false;
        }
        if (i == 0) {
            return false;
        }
        if (i > 1) {
            String tmp = expressionStr.substring(i - 2, i);
            if ((sign + "" + sign).equals(tmp)) {
                return false;
            }

        }
        if (i < expressionStr.length() - 1 && i > 0) {
            String tmp = expressionStr.substring(i - 1, i + 1);
            if ((sign + "" + sign).equals(tmp)) {
                return false;
            }
        }
        return true;
    }

    public static void main(String[] args) {
        String str = "splitarray('da''fsfds''ta','fdsdfs')";
        Map<String, String> flag2ExpressionStr = new HashMap<>(16);
        String value = doConstantReplace(str, flag2ExpressionStr, 1);
        String[] values = value.split(",");
        int i = 0;
        for (String v : values) {
            values[i] = restore(v, flag2ExpressionStr);
            System.out.println(values[i]);
        }
        System.out.println(value);
    }

    public static boolean containContant(String jsonValue) {
        if (isContant(jsonValue)) {
            return true;
        }
        if (jsonValue.indexOf("'") != -1) {
            return true;
        }
        return false;
    }

    public static boolean isContant(String jsonValue) {

        if (jsonValue.startsWith("'") && jsonValue.endsWith("'")) {
            return true;
        }
        return false;
    }

}
