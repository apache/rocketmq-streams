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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class NumberUtils {

    public static byte[] toByte(int n) {
        byte[] b = new byte[4];
        b[0] = (byte)(n & 0xff);
        b[1] = (byte)(n >> 8 & 0xff);
        b[2] = (byte)(n >> 16 & 0xff);
        b[3] = (byte)(n >> 24 & 0xff);
        return b;
    }

    public static int toInt(byte b) {
        byte[] bytes = new byte[1];
        bytes[0] = b;
        return toInt(bytes);
    }

    /**
     * byte[] to int, index 0 means low
     *
     * @param b
     * @return
     */
    public static int toInt(byte[] b) {
        int res = 0;
        for (int i = 0; i < b.length; i++) {
            res += (b[i] & 0xff) << (i * 8);
        }
        return res;
    }

    public static int toInt(byte[] globalBytes, int index) {
        byte[] bytes = getSubByteFromIndex(globalBytes, index, 4);
        return toInt(bytes);
    }

    /**
     * 把一个子字节数组，插入到大大字节数组中
     *
     * @param globalBytes 全局字节数组
     * @param index       从哪里插入
     * @param subBytes    待插入的子字节
     */
    public static void putSubByte2ByteArray(byte[] globalBytes, int index, byte[] subBytes) {
        if (index < 0 || subBytes == null) {
            return;
        }
        for (int i = 0; i < subBytes.length; i++) {
            globalBytes[i + index] = subBytes[i];
        }
    }

    /**
     * 从总字节中获取部分字节
     *
     * @param globalBytes
     * @param index
     * @param size
     * @return
     */
    public static byte[] getSubByteFromIndex(byte[] globalBytes, int index, int size) {
        if (index < 0 || size < 0) {
            return null;
        }
        byte[] subBytes = Arrays.copyOfRange(globalBytes, index, size + index);
        return subBytes;
    }

    /**
     * 把存储0/1字符串的值，转化成bit
     *
     * @param booleanValues
     * @return
     */
    public static int createBitMapInt(Boolean... booleanValues) {
        List<String> values = new ArrayList<>();
        for (Boolean value : booleanValues) {
            if (value != null && value == true) {
                values.add("1");
            } else {
                values.add(null);
            }

        }
        return createBitMapInt(values);
    }

    /**
     * 把存储0/1字符串的值，转化成bit
     *
     * @param values
     * @return
     */
    public static int createBitMapInt(List<String> values) {
        if (values == null) {
            return 0;
        }
        int result = 0;
        int i = 0;
        for (String value : values) {
            if (value != null && "1".equals(value)) {
                result = (result | (1 << i));
            }
            i++;
        }
        return result;
    }

    /**
     * 获取某位的值，如果是1，返回字符串1，否则返回null
     *
     * @param num
     * @param index
     * @return
     */
    public static boolean getNumFromBitMapInt(int num, int index) {
        boolean isTrue = ((num & (1 << index)) != 0);//true 表示第i位为1,否则为0
        return isTrue;
    }

    /**
     * 获取某位的值，如果是1，返回字符串1，否则返回null
     *
     * @param num
     * @param index
     * @return
     */
    public static int setNumFromBitMapInt(int num, int index) {
        num = (num | (1 << index));
        return num;
    }

    /**
     * 去掉小数点，防止序列化反序列化过程中出现类型不一致，因为计算过程中统一使用了double类型
     *
     * @param value
     * @return
     */
    public static Number stripTrailingZeros(double value) {
        BigDecimal decimal = new BigDecimal(value);
        BigDecimal result = decimal.stripTrailingZeros();
        int scale = result.scale();
        if (scale <= 0) {
            return decimal.intValue();
        } else {
            return result.doubleValue();
        }
    }

    public static void main(String[] args) {
        int num = createBitMapInt(true, false, false, false, true);
        num = setNumFromBitMapInt(num, 1);
        System.out.println(getNumFromBitMapInt(num, 4));

    }
}
