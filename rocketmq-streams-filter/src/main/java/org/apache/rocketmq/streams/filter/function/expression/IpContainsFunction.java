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

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.filter.utils.IPUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Function
public class IpContainsFunction extends AbstractExpressionFunction {

    private static final Logger LOGGER = LoggerFactory.getLogger(IpContainsFunction.class);

    private String ip;
    private long start;
    private long end;
    private String startIp;
    private String endIp;

    @SuppressWarnings("rawtypes")
    @Override
    @FunctionMethod("ipContains")
    @FunctionMethodAilas("ip包含")
    public Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression) {
        try {
            if (!expression.volidate()) {
                return false;
            }

            Var var = expression.getVar();
            if (var == null) {
                return false;
            }
            Object varObject = null;
            Object valueObject = null;
            varObject = var.doMessage(message,context );
            valueObject = expression.getValue();

            if (varObject == null || valueObject == null) {
                return false;
            }

            String varString = "";
            String regex = "";
            varString = String.valueOf(varObject).trim();
            regex = String.valueOf(valueObject).trim();

            if (StringUtil.isEmpty(varString) || StringUtil.isEmpty(regex)) {
                return false;
            }
            parseRegexIp(regex);
            return contains(varString);
        } catch (Exception e) {
            LOGGER.error("IpContainsFunction doExpressionFunction error", e);
            return false;
        }

    }

    private void parseRegexIp(String ip) {
        if (null == ip || "".equals(ip)) {
            return;
        }

        int n = ip.indexOf("/");
        String preIp = ip;
        int mask = 32;
        if (n > 0) {
            preIp = ip.substring(0, n);
            mask = Integer.parseInt(ip.substring(n + 1));
            if (!IPUtil.checkMask(mask)) {
                return;
            }
        }

        if (!IPUtil.checkIpFormat(preIp)) {
            return;
        }
        this.ip = ip;
        this.start = getStart(preIp, mask);
        this.end = getEnd(preIp, mask);
        this.startIp = IPUtil.ipToString(this.start);
        this.endIp = IPUtil.ipToString(this.end);
    }

    private long getStart(String ip, int mask) {
        return IPUtil.ipToInt(ip) & IPUtil.getPrefixIp(mask);
    }

    private long getEnd(String ip, int mask) {
        return getStart(ip, mask) | IPUtil.getSuffixIp(mask);
    }

    /*
     * 判断IP是否包含
     */
    private boolean contains(String ip) {
        // 先判断是否为ip段
        int n = ip.indexOf("/");
        if (n <= 0) {
            if (!IPUtil.checkIpFormat(ip)) {
                return false;
            }

            long intIp = IPUtil.ipToInt(ip);
            return this.start <= intIp && this.end >= intIp;
        }

        String preIp = ip.substring(0, n);
        int mask = Integer.parseInt(ip.substring(n + 1));
        if (!IPUtil.checkMask(mask) || !IPUtil.checkIpFormat(preIp)) {
            return false;
        }

        return this.start <= this.getStart(preIp, mask) && this.end >= this.getEnd(preIp, mask);
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public String getStartIp() {
        return startIp;
    }

    public void setStartIp(String startIp) {
        this.startIp = startIp;
    }

    public String getEndIp() {
        return endIp;
    }

    public void setEndIp(String endIp) {
        this.endIp = endIp;
    }

}
