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

public class Ip2LongUtil {

    public static String ipInt2DotDec(int ipInt) throws IllegalArgumentException {
        return ipLong2DotDec(((long) ipInt) & 0xFFFFFFFFL);
    }

    public static String ipLong2DotDec(long ipLong) throws IllegalArgumentException {
        if (ipLong < 0) {
            throw new IllegalArgumentException("argument must be positive");
        } else if (ipLong > 4294967295L) {
            throw new IllegalArgumentException("argument must be smaller than 4294967295L");
        }

        StringBuffer sb = new StringBuffer(15);
        sb.append((int) (ipLong >>> 24));
        sb.append('.');
        sb.append((int) ((ipLong & 0x00FFFFFF) >>> 16));
        sb.append('.');
        sb.append((int) ((ipLong & 0x0000FFFF) >>> 8));
        sb.append('.');
        sb.append((int) ((ipLong & 0x000000FF)));

        return sb.toString();
    }

    static int number(char ch) {
        int val = ch - '0';
        if (val < 0 || val > 9) {
            throw new IllegalArgumentException();
        }

        return val;
    }

    public static int ipDotDec2Int(String ip) {
        if (ip == null || ip.length() == 0) {
            throw new IllegalArgumentException("argument is null");
        }

        if (ip.length() > 15) {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }

        int i = 0;
        char ch = ip.charAt(i++);

        while (ch == ' ' && i < ip.length()) {
            ch = ip.charAt(i++);
        }

        int p0 = number(ch);

        ch = ip.charAt(i++);

        if (ch != '.') {
            p0 = p0 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            p0 = p0 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }

        ch = ip.charAt(i++);

        int p1 = number(ch);

        ch = ip.charAt(i++);

        if (ch != '.') {
            p1 = p1 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            p1 = p1 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }

        ch = ip.charAt(i++);

        int p2 = number(ch);

        ch = ip.charAt(i++);

        if (ch != '.') {
            p2 = p2 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            p2 = p2 * 10 + number(ch);
            ch = ip.charAt(i++);
        }

        if (ch != '.') {
            throw new IllegalArgumentException("illegal ip : " + ip);
        }

        ch = ip.charAt(i++);

        int p3 = number(ch);

        if (i < ip.length()) {
            ch = ip.charAt(i++);
            p3 = p3 * 10 + number(ch);
        }

        if (i < ip.length()) {
            ch = ip.charAt(i++);
            p3 = p3 * 10 + number(ch);
        }

        return (p0 << 24) + (p1 << 16) + (p2 << 8) + p3;
    }

    public static long ipDotDec2Long(String ipDotDec) throws IllegalArgumentException {
        int intValue = ipDotDec2Int(ipDotDec);

        return ((long) intValue) & 0xFFFFFFFFL;
    }

    public static String ipHex2DotDec(String ipHex) throws IllegalArgumentException {
        if (ipHex == null) {
            throw new IllegalArgumentException("argument is null");
        }

        ipHex = ipHex.trim();

        if (ipHex.length() != 8 && ipHex.length() != 7) {
            throw new IllegalArgumentException("argument is: " + ipHex);
        }

        long ipLong = Long.parseLong(ipHex, 16);

        return ipLong2DotDec(ipLong);
    }

    public static String ipDotDec2Hex(String ipDotDec) throws IllegalArgumentException {
        long ipLong = ipDotDec2Long(ipDotDec);

        String ipHex = Long.toHexString(ipLong);

        if (ipHex.length() < 8) {
            ipHex = "0" + ipHex;
        }

        return ipHex;
    }

    public static boolean ipCheck(String ip) {
        if (ip == null) {
            throw new IllegalArgumentException("argument is null");
        }

        if (ip.isEmpty()) {
            return false;
        }

        int p0 = ip.indexOf('.');
        int section0 = validate2(ip, 0, p0);
        if (section0 < 0) {
            return false;
        }

        int p1 = ip.indexOf('.', p0 + 1);
        int section1 = validate2(ip, p0 + 1, p1);
        if (section1 < 0) {
            return false;
        }

        int p2 = ip.indexOf('.', p1 + 1);
        int section2 = validate2(ip, p1 + 1, p2);
        if (section2 < 0) {
            return false;
        }

        int section3 = validate2(ip, p2 + 1, ip.length());
        if (section3 < 0) {
            return false;
        }

        int result = (section0 << 24) + (section1 << 16) + (section2 << 8) + section3;

        return result == 0 ? false : true;
    }

    static boolean validate(String text, int fromIndex, int endIndex) {
        if (endIndex == -1) {
            return false;
        }

        int len = endIndex - fromIndex;

        if (len < 1 || len > 3) {
            return false;
        }

        int value = 0;

        for (int i = fromIndex; i < endIndex; ++i) {
            char ch = text.charAt(i);

            int num = ch - '0';
            if (num < 0 || num > 9) {
                return false;
            }

            value = value * 10 + num;
        }

        if (value < 0 || value > 255) {
            return false;
        }

        return true;
    }

    static int validate2(String text, int fromIndex, int endIndex) {
        if (endIndex == -1) {
            return -1;
        }

        int len = endIndex - fromIndex;

        if (len < 1 || len > 3) {
            return -1;
        }

        int value = 0;

        for (int i = fromIndex; i < endIndex; ++i) {
            char ch = text.charAt(i);

            int num = ch - '0';
            if (num < 0 || num > 9) {
                return -1;
            }

            value = value * 10 + num;
        }

        if (value < 0 || value > 255) {
            return -1;
        }

        return value;
    }

    public Long ipDotDec2Long(String ip, Boolean ignoreError) {
        if (ip == null || ip.isEmpty()) {
            return null;
        }

        if (!ipCheck(ip)) {
            if (ignoreError == true) {
                return null;
            } else {
                if ("000.000.000.000".equals(ip)) {
                    return 0L;
                }
                throw new IllegalArgumentException("illegal ip : " + ip);
            }
        }

        return ipDotDec2Long(ip);
    }

}
