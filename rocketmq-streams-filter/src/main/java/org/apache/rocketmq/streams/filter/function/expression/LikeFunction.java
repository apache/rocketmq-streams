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
package org.apache.rocketmq.streams.filter.function.expression;

import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.LikeRegex;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class LikeFunction extends AbstractExpressionFunction {
    private transient ICache<String, LikeRegex> likeCache = new SoftReferenceCache<>();

    private transient ICache<String, LikeCache> cache = new SoftReferenceCache<>();

    public static boolean isLikeFunciton(String functionName) {
        return "like".equals(functionName);
    }

    private class LikeCache {
        public LikeCache(String containStr, String regexStr, boolean isPrefix) {
            this.containStr = containStr;
            this.regexStr = regexStr;
            this.isPrefix = isPrefix;

        }

        protected String containStr;//预处理like中一定包含的字符串
        protected String regexStr;//把like转换成regex对应的字符串
        protected boolean isPrefix = false;//是否是前缀匹配

    }

    @Override
    @FunctionMethod("like")
    @FunctionMethodAilas("包含")
    public Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression) {
        Var var = expression.getVar();
        if (var == null) {
            return false;
        }
        Object varObject = null;
        Object valueObject = null;
        varObject = var.doMessage(message,context);
        valueObject = expression.getValue();

        if (varObject == null || valueObject == null) {
            return false;
        }

        String varString = "";
        String valueString = "";
        varString = String.valueOf(varObject).trim();
        valueString = String.valueOf(valueObject).trim();
        if (StringUtil.isEmpty(valueString)) {
            return false;
        }
        valueString = FunctionUtils.getConstant(valueString);

        LikeRegex likeRegex = likeCache.get(valueString);
        if (likeRegex == null) {
            likeRegex = new LikeRegex(valueString);
            likeCache.put(valueString, likeRegex);
        }
        boolean likeResult = likeRegex.match(varString);
        // System.out.println(likeResult);
        return likeResult;

        //LikeCache likeCache=sinkcache.get(valueString);
        //if(likeCache==null){
        //    String containStr=parseContainStr(valueString);
        //    String regexStr=convertRegex(valueString);
        //    likeCache=new LikeCache(containStr,regexStr,!valueString.startsWith("%"));
        //    sinkcache.put(valueString,likeCache);
        //}
        //if(likeCache.containStr!=null){
        //    if(likeCache.isPrefix&&!varString.startsWith(likeCache.containStr)){
        //        return false;
        //    }
        //    if(varString.indexOf(likeCache.containStr)==-1){
        //        return false;
        //    }
        //}
        //boolean result= StringUtil.matchRegex(varString,likeCache.regexStr);
        //return result;
    }

    protected static String[] regexSpecialWords = {"\\", "$", "(", ")", "*", "+", ".", "[", "]", "?", "^", "{", "}", "|"};

    /**
     * 把like中一定包含的字符串抽取出来，再正则匹配前，先做预处理
     *
     * @param likeStr like的语句
     * @return 如果能够抽取，返回抽取的字符串，否则返回null
     */
    protected static String parseContainStr(String likeStr) {
        String tmp = likeStr;
        if (tmp.startsWith("%")) {
            tmp = tmp.substring(1);
        }
        if (tmp.endsWith("%")) {
            tmp = tmp.substring(0, tmp.length() - 1);
        }
        for (String word : regexSpecialWords) {
            if (tmp.contains(word)) {
                return null;
            }
        }
        return null;
    }

    /**
     * regexWord.add("."); regexWord.add("*"); regexWord.add("?"); regexWord.add("{"); regexWord.add("}"); // regexWord.add("\\d"); regexWord.add(")"); //        regexWord.add("("); regexWord.add("+"); regexWord.add("\\"); regexWord.add("["); regexWord.add("]"); regexWord.add("^"); regexWord.add("$");
     *
     * @param likeStr
     * @return
     */

    protected static String convertRegex(String likeStr) {
        likeStr = likeStr.replace("\\", "\\\\");
        likeStr = likeStr.replace("[!", "[^");
        likeStr = likeStr.replace(".", "\\.");
        likeStr = likeStr.replace("*", "\\*");
        likeStr = likeStr.replace("_", ".");
        likeStr = likeStr.replace("+", "\\+");

        if (!likeStr.trim().startsWith("%")) {
            likeStr = "^" + likeStr;
        }
        if (!likeStr.trim().endsWith("%")) {
            likeStr = likeStr + "$";
        }
        likeStr = likeStr.replace("%", ".*");
        return likeStr;
    }

    public static void main(String[] args) {
        String valueString = convertRegex("%/php%");
        System.out.println(valueString);
        System.out.println(StringUtil.matchRegex("/www/server/php/70/sbin/php-fpm", valueString));
    }
}
