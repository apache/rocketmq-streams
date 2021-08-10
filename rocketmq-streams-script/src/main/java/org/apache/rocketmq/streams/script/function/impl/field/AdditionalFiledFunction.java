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
package org.apache.rocketmq.streams.script.function.impl.field;

import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.Ip2LongUtils;
import org.apache.rocketmq.streams.common.utils.RandomStrUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.script.utils.MatchUtil;

@Function
public class AdditionalFiledFunction {

    private static final String RECEIVE_TIME = "sys_receive_time";                   // 在日志中加入接收时间
    private static final String STANDARD_FORMAT = "yyyy-MM-dd HH:mm:ss";                // 默认的时间格式
    private static final String RECEIVE_TIMESTAMP = "sys_receive_timestamp";              // 在日志中加入接收时间戳

    private SimpleDateFormat standardDateFormat = new SimpleDateFormat(STANDARD_FORMAT);

    private String aliuidConstant;

    @FunctionMethod(value = "addRandomId", alias = "addRandom", comment = "增加一个随机数的新字段")
    public String addRandomId(IMessage message, FunctionContext context,
                              @FunctionParamter(value = "string", comment = "新字段名称") String newFieldName,
                              @FunctionParamter(value = "string", comment = "字段长度，不需要单引号或双引号") String strLength) {
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        String random = random(message, context, strLength);
        message.getMessageBody().put(name, random);
        return random;
    }

    @FunctionMethod(value = "random", alias = "createRandom", comment = "产生一个随机数")
    public String random(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表字段长度的字段名，数字或常量") String strLength) {
        Long length = 10l;
        if (!StringUtil.isEmpty(strLength) && !"null".equals(strLength)) {
            Object object = FunctionUtils.getValue(message, context, strLength);
            if (FunctionUtils.isNumberObject(object)) {
                length = (Long)object;
            } else {
                length = Long.valueOf(object.toString());
            }
        }
        String random = RandomStrUtil.getRandomStr(length.intValue());
        return random;
    }

    @FunctionMethod(value = "copyField", alias = "copy", comment = "copy一个字段的值到一个新的字段（不建议使用，建议使用直接赋值方式）")
    public Object extra(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表新字段名的字段名") String newFieldName,
                        @FunctionParamter(value = "string", comment = "代表字段值的字段名或常量") String oldFieldName) {
        Object value = FunctionUtils.getValue(message, context, oldFieldName);
        if (value == null) {
            return null;
        }
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        message.getMessageBody().put(name, value);
        return value;
    }

    @FunctionMethod(value = "copyField", alias = "copy", comment = "copy一个字段的值到一个新的字段")
    public Object extra(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表字段值的字段名或常量") String oldFieldName) {
        Object value = FunctionUtils.getValue(message, context, oldFieldName);
        return value;
    }

    @FunctionMethod(value = "addReceiveTime", alias = "addNow", comment = "增加当前时间")
    public String addReceiveTime(IMessage message, FunctionContext context) {
        String receiveTime = createNowTime();
        Date now = new Date();
        message.getMessageBody().put(RECEIVE_TIME, standardDateFormat.format(now));
        message.getMessageBody().put(RECEIVE_TIMESTAMP, now.getTime());
        return receiveTime;
    }

    @FunctionMethod(alias = "addField", value = "newField", comment = "增加一个新字段，字段值为value")
    public String addField(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "新字段的名称，不需要引号") String newFieldName,
                           @FunctionParamter(value = "string", comment = "代表新字段值的字段名或常量") String value) {
        Object newFieldValue = FunctionUtils.getValue(message, context, value);
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        message.getMessageBody().put(name, newFieldValue);
        return value;
    }

    @Deprecated
    @FunctionMethod(value = "addConstant", alias = "constant", comment = "增加一个常量")
    public String addConstant(IMessage message, FunctionContext context,
                              @FunctionParamter(value = "string", comment = "新字段的名次，不需要引号") String newFieldName,
                              @FunctionParamter(value = "string", comment = "新字段的值，默认为常量，不需要引号") String value) {
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        String tmp = value;
        value = FunctionUtils.getValueString(message, context, value);
        if (StringUtil.isEmpty(value)) {
            value = tmp;
        }
        message.getMessageBody().put(name, value);
        return value;
    }

    /**
     * 根据某个字段是否等于一个值来设置 比如 addByEqual(is_abroad,country,86,0,1) 判断如果country==86则把is_abroad设置为0，否则设置成1
     *
     * @param message
     * @param context
     * @param newFieldName  "is_abroad"
     * @param msgFielName   "country"
     * @param compareValue  "86"
     * @param equalValue    "0"
     * @param notEqualValue "1"
     */
    @Deprecated
    @FunctionMethod(value = "addByEqual", comment = "建议用if(compareValue==b)"
        + "then{newFieldName=equalValue}else{newFieldName=notEqualValue}方式")
    public String addByEqual(IMessage message, FunctionContext context, String newFieldName, String msgFielName,
                             String compareValue, String equalValue, String notEqualValue) {
        String value = null;
        String msgValue = FunctionUtils.getValueString(message, context, msgFielName);
        if (compareValue.equals(msgValue)) {
            value = equalValue;
        } else {
            value = notEqualValue;
        }
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        message.getMessageBody().put(name, value);
        return value;
    }

    /**
     * 从配置文件里取常量值
     *
     * @param message
     * @param context
     * @param newFieldName
     * @return
     */
    @FunctionMethod("addAliuidConstant")
    public String addVarFromProperties(IMessage message, FunctionContext context, String newFieldName) {
        try {
            String name = FunctionUtils.getValueString(message, context, newFieldName);
            if (StringUtil.isEmpty(name)) {
                name = newFieldName;
            }
            if (StringUtil.isNotEmpty(aliuidConstant)) {
                message.getMessageBody().put(name, Long.parseLong(aliuidConstant));
                return aliuidConstant;
            }
        } catch (Exception e) {

        }
        return null;
    }

    /**
     * 把一个string类型的ip变成long型的
     *
     * @param message
     * @param context
     * @param newFieldName
     * @param msgFielName
     */
    @FunctionMethod(value = "addByIp2Long", comment = "把ip转换成long，并增加到新字段")
    public String addByIp2Long(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "string", comment = "新字段的名次，不需要引号") String newFieldName,
                               @FunctionParamter(value = "string", comment = "代表ip的字段名或常量") String msgFielName) {

        long ipLong = ip2Long(message, context, msgFielName);
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        message.getMessageBody().put(name, ipLong);
        return String.valueOf(ipLong);
    }

    /**
     * 把一个string类型的ip变成long型的
     *
     * @param message
     * @param context
     * @param msgFielName
     */
    @FunctionMethod(value = "ip2Long", comment = "把ip转换成long")
    public Long ip2Long(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表ip的字段名或常量") String msgFielName) {
        Object o = FunctionUtils.getValue(message, context, msgFielName);
        if (o == null) {
            return null;
        }
        String ip = (String)o;
        long ipLong = Ip2LongUtils.ipDotDec2Long(ip);
        return ipLong;
    }

    @FunctionMethod(value = "timeLong2String", comment = "把时间戳转换成标准格式，赋值给新字段")
    public String timeLong2String(IMessage message, FunctionContext context,
                                  @FunctionParamter(value = "string", comment = "新字段的名次，不需要引号") String newFieldName,
                                  @FunctionParamter(value = "string", comment = "代表时间的字段名或常量") String msgFielName) {
        Object o = FunctionUtils.getValue(message, context, msgFielName);
        if (o == null) {
            return null;
        }
        String time = (String)o;
        if (MatchUtil.isNumber(time)) {
            time = DateUtil.longToString(Long.parseLong(time));
        }
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        message.getMessageBody().put(name, time);
        return time;
    }

    /**
     * 做时间比对，如果事件的发生事件在cache时间之后可以打标，否则不可以
     *
     * @param occureTime
     * @param msgOccureTime
     * @return
     */
    private boolean matchOccureTime(Object occureTime, String cacheDateFormat, String msgOccureTime,
                                    String msgDateFormate) {
        Date openTime = createTime(occureTime, cacheDateFormat);
        Date logTime = createTime(msgOccureTime, msgDateFormate);
        long timeDiff = DateUtil.dateDiff(openTime, logTime);
        if (timeDiff <= 0) { // 说明ip开通时间在时间发生时间之前
            return true;
        }
        return false;
    }

    private Date createTime(Object time, String format) {
        if (String.class.isInstance(time)) {
            return createTime((String)time, format);
        } else if (Date.class.isInstance(time)) {
            return createTime((Date)time);
        } else if (Long.class.isInstance(time)) {
            return createTime((Long)time);
        }
        return null;
    }

    private Date createTime(long time) {
        Date date = new Date(time);
        return date;
    }

    private Date createTime(Date time) {
        return time;
    }

    private Date createTime(String time, String format) {
        if (format == null) {
            format = STANDARD_FORMAT;
        }
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = dateFormat.parse(time);
            return date;
        } catch (Exception e) {
            return null;
        }
    }

    private String createNowTime() {
        return standardDateFormat.format(new Date());
    }

    public String getAliuidConstant() {
        return aliuidConstant;
    }

    public void setAliuidConstant(String aliuidConstant) {
        this.aliuidConstant = aliuidConstant;
    }

}
