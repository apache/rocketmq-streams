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
package org.apache.rocketmq.streams.openapi.utils;

import com.aliyuncs.auth.AcsURLEncoder;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Calendar;
import java.util.SortedMap;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParamUtil {
    private static final String ENCODING = "UTF-8";
    private static final String MAC_NAME = "HmacSHA1";
    private static final Logger LOGGER = LoggerFactory.getLogger(ParamUtil.class);

    public static String getUrl(String domain, String sk, String HTTPMethod, SortedMap<String, Object> params) {
        //本地时间 与 服务器时间不能超过15分钟
        params.put("Timestamp", getUTCTimeStr());
        String signature = null;
        StringBuffer url = null;
        String SignatureNonce = null;
        try {
            //获取signature
            do {
                SignatureNonce = UUID.randomUUID().toString().replace("-", "").toUpperCase();
                params.put("SignatureNonce", SignatureNonce);
                url = new StringBuffer();
                url.append("https://").append(domain).append("/?");
                url.append(allParams(params));
                signature = hamcsha1(composeStringToSign(HTTPMethod, params).getBytes(ENCODING), sk.getBytes(ENCODING));
            }
            while (signature.contains("+"));
        } catch (UnsupportedEncodingException e) {
            LOGGER.error(" signature is:" + signature);
        }
        //url的拼接
        url.append("&").append("Signature").append("=").append(signature);
        return url.toString();
    }

    public static String hamcsha1(byte[] data, byte[] key) {
        try {
            SecretKeySpec signingKey = new SecretKeySpec(key, "HmacSHA1");
            Mac mac = Mac.getInstance("HmacSHA1");
            mac.init(signingKey);
            return Base64Utils.encode(mac.doFinal(data));
        } catch (Exception e) {
            LOGGER.error("hamc sha1 error", e);
        }
        return null;
    }

    private static String composeStringToSign(String method, SortedMap<String, Object> queryParameters) {
        try {
            String canonicalQueryString = queryParameters.entrySet().stream().map(entry -> percentEncode(entry.getKey()) + "=" + percentEncode(entry.getValue().toString())).collect(Collectors.joining("&"));

            LOGGER.info(canonicalQueryString);
            return method.toUpperCase() + "&" + AcsURLEncoder.percentEncode("/") + "&" + AcsURLEncoder.percentEncode(canonicalQueryString);
        } catch (UnsupportedEncodingException e) {
            LOGGER.error(" composeStringToSign error " + e);
            return null;
        }

    }

    private static String allParams(SortedMap<String, Object> queryParameters) {
        return queryParameters.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining("&"));
    }

    public static String percentEncode(String value) {
        try {
            return value != null ? URLEncoder.encode(value, ENCODING).replace("+", "%20").replace("*", "%2A").replace("%7E", "~") : null;
        } catch (UnsupportedEncodingException e) {
            LOGGER.error(" percentEncode error " + e);
            return null;
        }

    }

    private static String getUTCTimeStr() {
        StringBuffer UTCTimeBuffer = new StringBuffer();
        // 1、取得本地时间：
        Calendar cal = Calendar.getInstance();
        // 2、取得时间偏移量：
        int zoneOffset = cal.get(java.util.Calendar.ZONE_OFFSET);
        // 3、取得夏令时差：
        int dstOffset = cal.get(java.util.Calendar.DST_OFFSET);
        // 4、从本地时间里扣除这些差量，即可以取得UTC时间：
        cal.add(java.util.Calendar.MILLISECOND, -(zoneOffset + dstOffset));
        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int sec = cal.get(Calendar.SECOND);
        StringBuffer appendMonth = new StringBuffer();
        if (month < 10) {
            appendMonth.append("0").append(month);
        } else {
            appendMonth.append(month);
        }
        StringBuffer appendDay = new StringBuffer();
        if (day < 10) {
            appendDay.append("0").append(day);
        } else {
            appendDay.append(day);
        }
        StringBuffer appendHour = new StringBuffer();
        if (hour < 10) {
            appendHour.append("0").append(hour);
        } else {
            appendHour.append(hour);
        }
        StringBuffer appendMinute = new StringBuffer();
        if (minute < 10) {
            appendMinute.append("0").append(minute);
        } else {
            appendMinute.append(minute);
        }
        StringBuffer appendSec = new StringBuffer();
        if (sec < 10) {
            appendSec.append("0").append(sec);
        } else {
            appendSec.append(sec);
        }

        UTCTimeBuffer.append(year).append("-").append(appendMonth).append("-").append(appendDay);
        UTCTimeBuffer.append("T").append(appendHour).append(":").append(appendMinute).append(":").append(appendSec).append("Z");

        return UTCTimeBuffer.toString();
    }
}
