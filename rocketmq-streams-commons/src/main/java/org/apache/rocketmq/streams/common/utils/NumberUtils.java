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
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;

public class NumberUtils {
    public static byte[] toByte(int n,int byteSize) {
        byte[] b = new byte[byteSize];
        b[0] = (byte)(n & 0xff);
        b[1] = (byte)(n >> 8 & 0xff);
        b[2] = (byte)(n >> 16 & 0xff);
        b[3] = (byte)(n >> 24 & 0xff);
        return b;
    }
    public static byte[] toByte(int n) {
        return toByte(n,4);
    }

    public static byte[] toByte(long n) {
        byte[] b = new byte[8];
        b[0] = (byte)(n & 0xff);
        b[1] = (byte)(n >> 8 & 0xff);
        b[2] = (byte)(n >> 16 & 0xff);
        b[3] = (byte)(n >> 24 & 0xff);
        b[4] = (byte)(n >>32 & 0xff);
        b[5] = (byte)(n >> 40 & 0xff);
        b[6] = (byte)(n >> 48 & 0xff);
        b[7] = (byte)(n >> 56 & 0xff);
        return b;
    }


    public static int toInt(byte b) {
        byte[] bytes = new byte[1];
        bytes[0] = b;
        return toInt(bytes);
    }

    public static boolean isFirstBitZero(ByteArray byteArray){
        byte firstByte= byteArray.getByte(byteArray.getSize() - 1);
        return isFirstBitZero(firstByte);
    }


    public static boolean isFirstBitZero(Integer integer) {
        ByteArray byteArray=new ByteArray(NumberUtils.toByte(integer));
        return isFirstBitZero(byteArray);
    }

    public static boolean isFirstBitZero(byte firstByte){
        int conflictValue = NumberUtils.toInt(firstByte);
        int conflictFlag = conflictValue >> 7;
        if (conflictFlag == 1) {
            return false;
        } else {
           return true;
        }
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


    public static byte[] zlibCompress(byte[] input){
        Deflater deflater=new Deflater();
        byte[] output = new byte[input.length+10+new Double(Math.ceil(input.length*0.25f)).intValue()];
        Deflater compresser = new Deflater();
        compresser.setInput(input);
        compresser.finish();
        int compressedDataLength = compresser.deflate(output);
        compresser.end();
        return Arrays.copyOf(output, compressedDataLength);
    }
    /**
     * 解压缩
     * @param barr   须要解压缩的字节数组
     * @return
     * @throws Exception
     */
    public static byte[] zlibInfCompress(byte[] barr)throws Exception{
        byte[] result=new byte[2014];
        Inflater inf=new Inflater();
        inf.setInput(barr);
        int infLen=inf.inflate(result);
        inf.end();
        return Arrays.copyOf(result, infLen);
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
        String str="10.10.10.11";
        System.out.println(str.getBytes().length);
        byte[] bytes=zlibCompress(str.getBytes());
        System.out.println(bytes.length);

    }

}
