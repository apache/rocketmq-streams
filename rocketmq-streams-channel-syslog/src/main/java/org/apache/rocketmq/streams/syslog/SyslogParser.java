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
package org.apache.rocketmq.streams.syslog;

import java.util.Date;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IPUtil;

public class SyslogParser {

    /**
     * 把message中的日期解析出来
     *
     * @param message 包含日期的几种变种
     * @param date    日期数据
     * @return 去掉日期后的message信息
     */
    public static String parseDate(String message, Date date) {
        String dateFormat = "HH:mm:ss";
        String dateStr = DateUtil.format(date, dateFormat);
        int endIndex = message.indexOf(dateStr);
        if (endIndex != -1) {
            message = message.substring(endIndex + dateFormat.length()).trim();
        }
        String rear = "CST " + DateUtil.getYear(date);
        if (message.startsWith(rear)) {
            message = message.substring(rear.length()).trim();
        } else if (message.startsWith(DateUtil.getYear(date))) {
            message = message.substring(4).trim();
        }
        return message;
    }

    /**
     * 类似这种，mymachine su: 'su root' failed for lonvick on /dev/pts/8
     *
     * @param message message是去除日期后的message值
     * @return 去除host后的数据
     */
    public static String parseHost(String message) {
        String[] values = message.trim().split(" ");
        if (values.length == 1) {
            return message;
        } else {
            String value = values[0];
            if (value.trim().endsWith(":")) {
                return message;
            }
            if (IPUtil.isValidIp(values[1])) {
                int endIndx = message.indexOf(values[1]) + values[1].length();
                message = message.substring(endIndx);
                return message.trim();
            }
            int endIndx = message.indexOf(value) + value.length();
            message = message.substring(endIndx);
        }
        return message.trim();
    }

    /**
     * 对于去除了时间，host的messag，解析tags
     *
     * @param message
     * @return tags
     */
    public static String parseTags(String message) {
        int index = message.indexOf(": ");
        if (index == -1) {
            return null;
        }
        String tags = message.substring(0, index);
        return tags;
    }

    public static void main(String[] args) {
        String dateFormat = "HH:mm:ss";
        String dateStr = DateUtil.format(new Date(), dateFormat);

        System.out.println(parseHost(dateStr));
    }
}
