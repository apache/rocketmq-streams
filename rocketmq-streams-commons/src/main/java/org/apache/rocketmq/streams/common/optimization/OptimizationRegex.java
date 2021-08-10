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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class OptimizationRegex {
    private static int orWordCount = 21;//对于or，超过多少就不处理了

    private static String[] regexSpecialWords = {"\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"};
    //正则表达式特殊字符
    private static String[] replaceSpecialWords = {"&", "@", "~"};//用于替换转义字符的特殊字符

    private static String[] regexSpecialWordsForSplit = {"$", "*", "+", ".", "?", "^"};//分割单词的分割符

    //正则表达式，对应的关键词优化器
    public static Map<String, OptimizationRegex> optimizationRegexMap = new HashMap<>();

    protected String regex;//原来的正则表达式
    protected boolean supportOptimizate = false;//是否支持优化
    protected List<String> andWords = new ArrayList<>();//and 关系的关键词
    protected List<List<String>> orWords = new ArrayList<>();//or 关系的关键词，外层的list是and关系，内层的词是or关系
    protected Map<Integer, String> orIndex2Keyword = new HashMap<>();//or 拼装的keyword，在list中的index
    protected Map<String, AtomicInteger> keyword2Count = new HashMap<>();//根据执行情况，动态数据快速失败的词，优化顺序

    public OptimizationRegex(String regex) {
        this.regex = regex;
        List<String> words = parseContainWords(regex);
        if (words != null && words.size() > 0) {
            supportOptimizate = true;
            //关键词都是and关系，如果某个关键词带|,则放到orwords中，orwords和andwords也是and关系
            int index = 0;
            for (String word : words) {
                if (word.contains("|")) {
                    List<String> tmp = new ArrayList<>();
                    String[] values = word.split("\\|");
                    for (String value : values) {

                        tmp.add(value);
                        //每个词一个优化统计计数器
                        keyword2Count.put(value, new AtomicInteger(0));
                    }
                    //多个or词拼装成字符串，便于快速查找
                    List<String> sortTmp = new ArrayList<>();
                    sortTmp.addAll(tmp);
                    Collections.sort(sortTmp);
                    String keyword = MapKeyUtil.createKey("|", sortTmp);
                    keyword2Count.put(keyword, new AtomicInteger(0));
                    orIndex2Keyword.put(orWords.size(), keyword);//list索引对应的keyword

                    orWords.add(tmp);

                } else {
                    //每个词一个优化统计计数器
                    keyword2Count.put(word, new AtomicInteger(0));
                    if (andWords.contains(word) == false) {
                        andWords.add(word);
                    }
                }
            }
        }
        //String regex2Optimization=regex+":"+this.toString();
        //List<String> regexs=new ArrayList<>();
        //regexs.add(regex2Optimization);
        //FileUtil.write("/Users/yuanxiaodong/Documents/dipper_engine/regex.txt",regexs,true);

    }

    /**
     * 通过关键词快速匹配，但不做正则部分的匹配
     *
     * @param content
     * @return
     */
    public boolean quickMatch(String content) {
        if (supportOptimizate == false) {
            return true;
        }
        for (String word : andWords) {
            if (content.indexOf(word) == -1) {
                //AtomicInteger count=keyword2Count.get(word);
                //count.incrementAndGet();
                return false;
            }
        }
        int index = 0;
        int contentLen = content.length();
        for (List<String> orWord : orWords) {
            boolean isMatch = false;
            int len = orWord.size();
            for (int i = 0; i < len; i++) {
                String word = orWord.get(i);
                if (contentLen > word.length() && content.indexOf(word) != -1) {
                    isMatch = true;
                    //AtomicInteger count=keyword2Count.get(word);
                    //count.incrementAndGet();
                    break;
                }
            }
            if (!isMatch) {
                //String keyword=orIndex2Keyword.get(index);
                //AtomicInteger count=keyword2Count.get(keyword);
                //count.incrementAndGet();
                return false;
            }
            //index++;
        }
        return true;
    }

    public List<String> parseContainWords(String regex) {
        String replaceSpecialWord = null;
        for (String word : replaceSpecialWords) {//先把所有的转义字符替换成特殊字符
            if (regex.indexOf(word) == -1) {
                replaceSpecialWord = word;
                break;
            }
        }
        return parseContainWords(regex, replaceSpecialWord);
    }

    /**
     * 把正则表达式，进行处理，对于不解析的特殊字符，先替换成replaceSpecialWord，以免造成干扰
     *
     * @param regex
     * @param replaceSpecialWord
     * @return
     */
    protected List<String> parseContainWords(String regex, String replaceSpecialWord) {

        //先替换掉转义字符
        String regexStr = transferredMeaning(regex, replaceSpecialWord);

        //把大中小扩号，替换成特殊字符串，带括号的先不做解析
        String bracketCancelContent = cancelBracket(regexStr, replaceSpecialWord, "(", ")");//去除掉括号部分
        bracketCancelContent = cancelBracket(bracketCancelContent, replaceSpecialWord, "[", "]");//去除掉括号部分
        bracketCancelContent = cancelBracket(bracketCancelContent, replaceSpecialWord, "{", "}");//去除掉括号部分

        if (bracketCancelContent.indexOf("|") == -1) {
            /**
             * 根据正则的特殊字符，划分成多个词，并最终完成词的选择和优化
             */
            return parseWord(bracketCancelContent, replaceSpecialWord);
        } else {
            //如果是有|的关系，只有在每个|都能抽取一个word的时候，才会做优化。只要有一个抽取不到关键词，或抽取了多个，都不做优化
            String[] values = bracketCancelContent.split("\\|");//把或分开
            if (values.length > orWordCount) {//如果超过n个|，就不做优化了e
                return null;
            } else {
                String result = "";
                boolean isFirst = true;
                for (String value : values) {
                    List<String> words = parseContainWords(value, replaceSpecialWord);
                    if (words == null || words.size() == 0) {//如果或的每一项没有抽取到关键词或超过一个关键词，都不再继续做优化
                        return null;
                    }
                    if (isFirst) {
                        isFirst = false;
                    } else {
                        result = result + "|";
                    }
                    result = result + words.get(0);
                }
                if (StringUtil.isEmpty(result)) {
                    return null;
                }
                List<String> words = new ArrayList<>();
                words.add(result);
                return words;
            }
        }

    }

    /**
     * 通过正则分割符号，找到前缀匹配关键词
     *
     * @param bracketCancelContent
     * @return
     */
    protected List<String> parseWord(String bracketCancelContent, String replaceSpecialWord) {
        StringBuilder sb = new StringBuilder();
        List<String> result = new ArrayList<>();
        for (int i = 0; i < bracketCancelContent.length(); i++) {
            String word = bracketCancelContent.substring(i, i + 1);
            boolean isSpecialWord = false;
            for (String specialWord : regexSpecialWordsForSplit) {
                if (word.equals(specialWord) || replaceSpecialWord.equals(word)) {
                    String regexWord = sb.toString();
                    sb = new StringBuilder();
                    if (StringUtil.isNotEmpty(regexWord) && regexWord.length() > 2) {
                        result.add(regexWord);
                    }
                    isSpecialWord = true;
                    continue;
                }
            }
            if (isSpecialWord == false) {
                sb.append(word);
            }
        }
        String regexWord = sb.toString();
        if (StringUtil.isNotEmpty(regexWord) && regexWord.length() > 2) {
            result.add(regexWord);
        }
        if (result.size() == 0) {
            return null;
        }
        return result;
    }

    /**
     * 把转义字符，替换成特殊字符
     *
     * @param regex              正则表达式
     * @param replaceSpecialWord 选取一个特殊字符
     * @return
     */
    protected String transferredMeaning(String regex, String replaceSpecialWord) {
        int index = regex.indexOf("\\");
        while (index != -1) {
            String word = regex.substring(index, index + 2);
            regex = regex.replace(word, replaceSpecialWord);
            index = regex.indexOf("\\");
        }
        return regex;
    }

    /**
     * 把括号替换成特殊字符串，括号里面的内容不做特殊处理
     *
     * @param regex              正则表达式
     * @param replaceSpecialWord 需要替换的特殊字符串
     * @param startBracket       开括号
     * @param endBracket         闭括号
     * @return
     */
    protected String cancelBracket(String regex, String replaceSpecialWord, String startBracket, String endBracket) {
        if (regex.indexOf(startBracket) == -1 || regex.indexOf(endBracket) == -1) {
            return regex;
        }
        int endIndex = regex.indexOf(endBracket);
        if (endIndex == -1) {
            return regex;
        }
        String buildBuffer = "";

        for (int i = endIndex; i > 0; i--) {
            String word = regex.substring(i - 1, i);
            if (word.equals(startBracket)) {
                String keyword = buildBuffer;
                if (StringUtil.isNotEmpty(keyword)) {
                    regex = regex.replace(startBracket + keyword + endBracket, replaceSpecialWord);
                }
            } else {
                buildBuffer = word + buildBuffer;
            }
        }
        return cancelBracket(regex, replaceSpecialWord, startBracket, endBracket);
    }

    @Override
    public String toString() {
        if ((orWords == null || orWords.size() == 0) && (andWords == null || andWords.size() == 0)) {
            return null;
        }
        List<String> allWordList = new ArrayList<>();
        for (List<String> list : orWords) {
            Collections.sort(list);
            String orWord = MapKeyUtil.createKey("|", list);
            allWordList.add(orWord);
        }
        allWordList.addAll(andWords);
        Collections.sort(allWordList);
        return MapKeyUtil.createKey("&", allWordList);
    }

    public static void main(String[] args) {
        String regex = ("php-fpm|fpm-php|php-cgi|java|apache|httpd|yarn|mysql|oracle|redis|hadoop|nginx|websphere|jboss|jenkins|tomcat|elasticsearch|oracledatabase|oracleweblogic").replace("\\\\", "\\");
        OptimizationRegex optimizationRegex = new OptimizationRegex(regex);
        //Boolean value= optimizationRegex.match("mv fdsld dofdsfpreload");
        //System.out.println(value);
        System.out.println(optimizationRegex);

    }

    public boolean isSupportOptimizate() {
        return supportOptimizate;
    }
}
