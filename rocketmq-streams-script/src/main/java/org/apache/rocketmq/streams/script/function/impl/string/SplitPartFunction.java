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
package org.apache.rocketmq.streams.script.function.impl.string;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class SplitPartFunction {

    /**
     * 依照分隔符separator拆分字符串str，返回从第start部分到第end部分的子串(闭区间)
     *
     * @param message
     * @param context
     * @param str
     * @param separator
     * @param startStr
     * @return
     */
    @FunctionMethod(value = "splitpart", comment = "依照分隔符separator拆分字符串str，返回从第start部分到第end部分的子串(闭区间)")
    public String splitpart(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "待拆分的源字段代表列字段或常量值") String str,
        @FunctionParamter(value = "string", comment = "拆分字段使用的字符") String separator,
        @FunctionParamter(value = "string", comment = "返回拆分结果的开始位置") String startStr) {
        StringBuilder sb = new StringBuilder();
        String ori = FunctionUtils.getValueString(message, context, str);
        separator = FunctionUtils.getValueString(message, context, separator);
        Integer start = FunctionUtils.getValueInteger(message, context, startStr);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(separator)) {
            return sb.toString();
        }
        if (start == null) {
            start = 0;
        }
        String strTem[] = str.split(separator);
        for (int i = start.intValue(); i < strTem.length; i++) {
            sb.append(strTem[i]);
        }
        return sb.toString();
    }

    /**
     * 依照分隔符separator拆分字符串str，返回从第start部分到第end部分的子串(闭区间)
     *
     * @param message
     * @param context
     * @param str
     * @param separator
     * @param startStr
     * @param endStr
     * @return
     */
    @FunctionMethod(value = "splitpart", comment = "依照分隔符separator拆分字符串str，返回从第start部分到第end部分的子串(闭区间)")
    public String splitpart(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "待拆分的源字段代表列字段或常量值") String str,
        @FunctionParamter(value = "string", comment = "拆分字段使用的字符") String separator,
        @FunctionParamter(value = "string", comment = "返回拆分结果的开始位置") String startStr,
        @FunctionParamter(value = "string", comment = "返回拆分结果的结束位置") String endStr) {
        StringBuilder sb = new StringBuilder();
        String ori = FunctionUtils.getValueString(message, context, str);
        separator = FunctionUtils.getValueString(message, context, separator);
        Integer start = FunctionUtils.getValueInteger(message, context, startStr);
        Integer end = FunctionUtils.getValueInteger(message, context, endStr);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(separator)) {
            return sb.toString();
        }
        if (start == null) {
            start = 0;
        }
        String strTem[] = str.split(separator);
        if (end > strTem.length - 1) {
            end = strTem.length - 1;
        }
        for (int i = start.intValue(); i <= end; i++) {
            sb.append(strTem[i]);
        }
        return sb.toString();
    }

    /**
     * 依照分隔符separator拆分字符串str，返回从第start部分到第end部分的子串(闭区间)
     *
     * @param message
     * @param context
     * @param str
     * @param separator
     * @return
     */
    @FunctionMethod(value = "splitindex", comment = "以sep作为分隔符，将字符串str分隔成若干段，取其中的第index段")
    public String splitindex(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "原字符串代表列名称或常量值") String str,
        @FunctionParamter(value = "string", comment = "处理源字段使用的字符") String separator,
        @FunctionParamter(value = "long", comment = "要返回值的索引") String indexStr) {
        String sb = null;
        String ori = FunctionUtils.getValueString(message, context, str);
        separator = FunctionUtils.getValueString(message, context, separator);
        Integer index = FunctionUtils.getValueInteger(message, context, indexStr);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(separator)) {
            return null;
        }
        if (index == null) {
            return null;
        }
        String strTem[] = ori.split(separator);
        if (index > strTem.length - 1 || index < 0) {
            return null;
        }
        sb = strTem[index.intValue()];
        return sb;
    }
}
