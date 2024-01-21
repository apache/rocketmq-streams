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
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;

public class NumberUtils {

    //todo 精度损失, 比如 3.2/9.6
    public static byte[] float2Bytes(float number) {
        int i = Float.floatToIntBits(number);
        byte[] b = new byte[4];
        for (int j = 0; j < 4; j++) {
            b[j] = (byte) (i >> (24 - j * 8));
        }
        System.out.println(i);
        return b;
    }

    public static float bytes2Float(byte[] bytes) {
        int i = (((int) bytes[0]) << 24) + (((int) bytes[1]) << 16)
            + (((int) bytes[2]) << 8) + bytes[3];
        System.out.println(i);
        return Float.intBitsToFloat(i);
    }

    /**
     * BIG_ENDIAN
     *
     * @param n
     * @param mask
     * @return
     */
    public static byte[] toByteArray(int n, int mask) {
        int arrayLength = 4;
        if (mask == 0xff) {
            arrayLength = 1;
        } else if (mask == 0xffff) {
            arrayLength = 2;
        } else if (mask == 0xffffff) {
            arrayLength = 3;
        } else if (mask == 0xffffffff) {
            arrayLength = 4;
        } else {
            throw new RuntimeException("mask must be '0xff, 0xffff, 0xffffff, 0xffffffff.'");
        }
        n = n & mask;
        byte[] b = new byte[arrayLength];
        for (int i = 0; i < arrayLength; i++) {
            int shift = i;
            if (shift > 0) {
                shift = 8 * shift;
                n = (n >> shift);
            }
            b[i] = (byte) (n & 0xff);
        }
        return b;
    }

    public static byte[] toByte(int n, int byteSize) {
        byte[] b = new byte[byteSize];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        return b;
    }

    public static byte[] toByte(int n) {
        return toByte(n, 4);
    }

    public static byte[] toByte(long n) {
        byte[] b = new byte[8];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);
        b[3] = (byte) (n >> 24 & 0xff);
        b[4] = (byte) (n >> 32 & 0xff);
        b[5] = (byte) (n >> 40 & 0xff);
        b[6] = (byte) (n >> 48 & 0xff);
        b[7] = (byte) (n >> 56 & 0xff);
        return b;
    }

    /**
     * byte[] to int, index 0 means low
     *
     * @param b
     * @return
     */
    public static long toLong(byte[] b) {
        long res = 0;
        int i = 0;
        for (; i < 8; i++) {
            byte byteValue = 0;
            if (i < b.length) {
                byteValue = b[i];
                res += (byteValue & 0xff) << (i * 8);
            } else {
                res += (byteValue & 0) << (i * 8);
            }

        }

        return res;
    }

    public static int toInt(byte b) {
        byte[] bytes = new byte[1];
        bytes[0] = b;
        return toInt(bytes);
    }

    public static boolean isFirstBitZero(ByteArray byteArray) {
        byte firstByte = byteArray.getByte(byteArray.getSize() - 1);
        return isFirstBitZero(firstByte);
    }

    public static boolean isFirstBitZero(Integer integer) {
        ByteArray byteArray = new ByteArray(NumberUtils.toByte(integer));
        return isFirstBitZero(byteArray);
    }

    public static boolean isFirstBitZero(byte firstByte) {
        int conflictValue = NumberUtils.toInt(firstByte);
        int conflictFlag = conflictValue >> 7;
        if (conflictFlag == 1) {
            return false;
        } else {
            return true;
        }
    }

    public static int toInt(byte[] b, int startIndex, int size) {
        int res = 0;
        for (int i = 0; i < size; i++) {
            res += (b[i + startIndex] & 0xff) << (i * 8);
        }
        return res;
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

    //添加input的length
    public static byte[] zlibCompress(byte[] input) {
//        try {
//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//            GZIPOutputStream gzip = new GZIPOutputStream(out);
//            gzip.write(input);
//            gzip.close();
//            byte[] dataArray = out.toString("ISO-8859-1").getBytes("ISO-8859-1");
////            return dataArray;
//            byte[] lengthArray = toByteArray(dataArray.length, 0xffff);
//            byte[] ret = new byte[dataArray.length + lengthArray.length];
//            ret[0] = lengthArray[0];
//            ret[1] = lengthArray[1];
//            System.arraycopy(dataArray, 0, ret, 2, dataArray.length);
//            return ret;
//        } catch (IOException e) {
//            e.printStackTrace();
//            return null;
//        }

//        byte[] output = new byte[input.length + 10 + new Double(Math.ceil(input.length*0.25f)).intValue()];
        if (input == null || input.length == 0) {
            return new byte[0];
        }
        int length = input.length;
        if (length < 256) {
            length = length * 3 + 10;
        } else {
            length = input.length + 10 + new Double(Math.ceil(input.length * 0.25f)).intValue();
        }
        byte[] output = new byte[length];
        Deflater compresser = new Deflater();
        compresser.setInput(input);
        compresser.finish();
        int compressedDataLength = compresser.deflate(output);
        byte[] lengthArray = toByteArray(input.length, 0xffff);
        byte[] ret = new byte[compressedDataLength + lengthArray.length];
        ret[0] = lengthArray[0];
        ret[1] = lengthArray[1];
        compresser.end();
        System.arraycopy(output, 0, ret, 2, compressedDataLength);
        return ret;
    }

    /**
     * 解压缩
     *
     * @param barr 须要解压缩的字节数组
     * @return
     * @throws Exception
     */
    public static byte[] zlibInfCompress(byte[] barr) throws DataFormatException {
        if (barr == null || barr.length == 0) {
            return new byte[0];
        }
        int length = (int) (barr[0] & 0xff) + ((barr[1] & 0xff) << 8);
        byte[] data = new byte[barr.length - 2];
        System.arraycopy(barr, 2, data, 0, data.length);
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        ByteArrayInputStream in = new ByteArrayInputStream(data);
//        GZIPInputStream gzip1 = new GZIPInputStream(in);
//        byte[] output = new byte[length];
//        int n;
//        while( (n = gzip1.read(output)) >= 0){
//            out.write(output, 0, n);
//        }
//        return output;
        byte[] result = new byte[length];
        Inflater inf = new Inflater();
        inf.setInput(data);
        int infLen = inf.inflate(result);
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

    public static void main(String[] args) throws Exception {
        String[] arrs = new String[8];
        arrs[0] = "ab947a25-d5ae-41c7-9675-1a34b66c55aa_30312_rg cnt/20211101/202111010000.xz cnt/20211101/202111010001.xz cnt/20211101/202111010002.xz cnt/20211101/202111010003.xz cnt/20211101/202111010004.xz cnt/20211101/202111010005.xz cnt/20211101/202111010006.xz cnt/20211101/202111010007.xz cnt/20211101/202111010008.xz cnt/20211101/202111010009.xz cnt/20211101/202111010010.xz cnt/20211101/202111010011.xz cnt/20211101/202111010012.xz cnt/20211101/202111010013.xz cnt/20211101/202111010014.xz cnt/20211101/202111010015.xz cnt/20211101/202111010016.xz cnt/20211101/202111010017.xz cnt/20211101/202111010018.xz cnt/20211101/202111010019.xz cnt/20211101/202111010020.xz cnt/20211101/202111010021.xz cnt/20211101/202111010022.xz cnt/20211101/202111010023.xz cnt/20211101/202111010024.xz cnt/20211101/202111010025.xz cnt/20211101/202111010026.xz cnt/20211101/202111010027.xz cnt/20211101/202111010028.xz cnt/20211101/202111010029.xz cnt/20211101/202111010030.xz cnt/20211101/202111010031.xz cnt/20211101/202111010032.xz cnt/20211101/202111010033.xz cnt/20211101/202111010034.xz cnt/20211101/202111010035.xz cnt/20211101/202111010036.xz cnt/20211101/202111010037.xz cnt/20211101/202111010038.xz cnt/20211101/202111010039.xz cnt/20211101/202111010040.xz cnt/20211101/202111010041.xz cnt/20211101/202111010042.xz cnt/20211101/202111010043.xz cnt/20211101/202111010044.xz cnt/20211101/202111010045.xz cnt/20211101/202111010046.xz cnt/20211101/202111010047.xz cnt/20211101/202111010048.xz cnt/20211101/202111010049.xz cnt/20211101/202111010050.xz cnt/20211101/202111010051.xz cnt/20211101/202111010052.xz cnt/20211101/202111010053.xz cnt/20211101/202111010054.xz cnt/20211101/202111010055.xz cnt/20211101/202111010056.xz cnt/20211101/202111010057.xz cnt/20211101/202111010058.xz cnt/20211101/202111010059.xz cnt/20211101/202111010100.xz cnt/20211101/202111010101.xz cnt/20211101/202111010102.xz cnt/20211101/202111010103.xz cnt/20211101/202111010104.xz cnt/20211101/202111010105.xz cnt/20211101/202111010106.xz cnt/20211101/202111010107.xz cnt/20211101/202111010108.xz cnt/20211101/202111010109.xz cnt/20211101/202111010110.xz cnt/20211101/202111010111.xz cnt/20211101/202111010112.xz cnt/20211101/202111010113.xz cnt/20211101/202111010114.xz cnt/20211101/202111010115.xz cnt/20211101/202111010116.xz cnt/20211101/202111010117.xz cnt/20211101/202111010118.xz cnt/20211101/202111010119.xz cnt/20211101/202111010120.xz cnt/20211101/202111010121.xz cnt/20211101/202111010122.xz cnt/20211101/202111010123.xz cnt/20211101/202111010124.xz cnt/20211101/202111010125.xz cnt/20211101/202111010126.xz cnt/20211101/202111010127.xz cnt/20211101/202111010128.xz cnt/20211101/202111010129.xz cnt/20211101/202111010130.xz cnt/20211101/202111010131.xz cnt/20211101/202111010132.xz cnt/20211101/202111010133.xz cnt/20211101/202111010134.xz cnt/20211101/202111010135.xz cnt/20211101/202111010136.xz cnt/20211101/202111010137.xz cnt/20211101/202111010138.xz cnt/20211101/202111010139.xz cnt/20211101/202111010140.xz cnt/20211101/202111010141.xz cnt/20211101/202111010142.xz cnt/20211101/202111010143.xz cnt/20211101/202111010144.xz cnt/20211101/202111010145.xz cnt/20211101/202111010146.xz cnt/20211101/202111010147.xz cnt/20211101/202111010148.xz cnt/20211101/202111010149.xz cnt/20211101/202111010150.xz cnt/20211101/202111010151.xz cnt/20211101/202111010152.xz cnt/20211101/202111010153.xz cnt/20211101/202111010154.xz cnt/20211101/202111010155.xz cnt/20211101/202111010156.xz cnt/20211101/202111010157.xz cnt/20211101/202111010158.xz cnt/20211101/202111010159.xz cnt/20211101/202111010200.xz cnt/20211101/202111010201.xz cnt/20211101/202111010202.xz cnt/20211101/202111010203.xz cnt/20211101/202111010204.xz cnt/20211101/202111010205.xz cnt/20211101/202111010206.xz cnt/20211101/202111010207.xz cnt/20211101/202111010208.xz cnt/20211101/202111010209.xz cnt/20211101/202111010210.xz cnt/20211101/202111010211.xz cnt/20211101/202111010212.xz cnt/20211101/202111010213.xz cnt/20211101/202111010214.xz cnt/20211101/202111010215.xz cnt/20211101/202111010216.xz cnt/20211101/202111010217.xz cnt/20211101/202111010218.xz cnt/20211101/202111010219.xz cnt/20211101/202111010220.xz cnt/20211101/202111010221.xz cnt/20211101/202111010222.xz cnt/20211101/202111010223.xz cnt/20211101/202111010224.xz cnt/20211101/202111010225.xz cnt/20211101/202111010226.xz cnt/20211101/202111010227.xz cnt/20211101/202111010228.xz cnt/20211101/202111010229.xz cnt/20211101/202111010230.xz cnt/20211101/202111010231.xz cnt/20211101/202111010232.xz cnt/20211101/202111010233.xz cnt/20211101/202111010234.xz cnt/20211101/202111010235.xz cnt/20211101/202111010236.xz cnt/20211101/202111010237.xz cnt/20211101/202111010238.xz cnt/20211101/202111010239.xz cnt/20211101/202111010240.xz cnt/20211101/202111010241.xz cnt/20211101/202111010242.xz cnt/20211101/202111010243.xz cnt/20211101/202111010244.xz cnt/20211101/202111010245.xz cnt/20211101/202111010246.xz cnt/20211101/202111010247.xz cnt/20211101/202111010248.xz cnt/20211101/202111010249.xz cnt/20211101/202111010250.xz cnt/20211101/202111010251.xz cnt/20211101/202111010252.xz cnt/20211101/202111010253.xz cnt/20211101/202111010254.xz cnt/20211101/202111010255.xz cnt/20211101/202111010256.xz cnt/20211101/202111010257.xz cnt/20211101/202111010258.xz cnt/20211101/202111010259.xz cnt/20211101/202111010300.xz cnt/20211101/202111010301.xz cnt/20211101/202111010302.xz cnt/20211101/202111010303.xz cnt/20211101/202111010304.xz cnt/20211101/202111010305.xz cnt/20211101/202111010306.xz cnt/20211101/202111010307.xz cnt/20211101/202111010308.xz cnt/20211101/202111010309.xz cnt/20211101/202111010310.xz cnt/20211101/202111010311.xz cnt/20211101/202111010312.xz cnt/20211101/202111010313.xz cnt/20211101/202111010314.xz cnt/20211101/202111010315.xz cnt/20211101/202111010316.xz cnt/20211101/202111010317.xz cnt/20211101/202111010318.xz cnt/20211101/202111010319.xz cnt/20211101/202111010320.xz cnt/20211101/202111010321.xz cnt/20211101/202111010322.xz cnt/20211101/202111010323.xz cnt/20211101/202111010324.xz cnt/20211101/202111010325.xz cnt/20211101/202111010326.xz cnt/20211101/202111010327.xz cnt/20211101/202111010328.xz cnt/20211101/202111010329.xz cnt/20211101/202111010330.xz cnt/20211101/202111010331.xz cnt/20211101/202111010332.xz cnt/20211101/202111010333.xz cnt/20211101/202111010334.xz cnt/20211101/202111010335.xz cnt/20211101/202111010336.xz cnt/20211101/202111010337.xz cnt/20211101/202111010338.xz cnt/20211101/202111010339.xz cnt/20211101/202111010340.xz cnt/20211101/202111010341.xz cnt/20211101/202111010342.xz cnt/20211101/202111010343.xz cnt/20211101/202111010344.xz cnt/20211101/202111010345.xz cnt/20211101/202111010346.xz cnt/20211101/202111010347.xz cnt/20211101/202111010348.xz cnt/20211101/202111010349.xz cnt/20211101/202111010350.xz cnt/20211101/202111010351.xz cnt/20211101/202111010352.xz cnt/20211101/202111010353.xz cnt/20211101/202111010354.xz cnt/20211101/202111010355.xz cnt/20211101/202111010356.xz cnt/20211101/202111010357.xz cnt/20211101/202111010358.xz cnt/20211101/202111010359.xz cnt/20211101/202111010400.xz cnt/20211101/202111010401.xz cnt/20211101/202111010402.xz cnt/20211101/202111010403.xz cnt/20211101/202111010404.xz cnt/20211101/202111010405.xz cnt/20211101/202111010406.xz cnt/20211101/202111010407.xz cnt/20211101/202111010408.xz cnt/20211101/202111010409.xz cnt/20211101/202111010410.xz cnt/20211101/202111010411.xz cnt/20211101/202111010412.xz cnt/20211101/202111010413.xz cnt/20211101/202111010414.xz cnt/20211101/202111010415.xz cnt/20211101/202111010416.xz cnt/20211101/202111010417.xz cnt/20211101/202111010418.xz cnt/20211101/202111010419.xz cnt/20211101/202111010420.xz cnt/20211101/202111010421.xz cnt/20211101/202111010422.xz cnt/20211101/202111010423.xz cnt/20211101/202111010424.xz cnt/20211101/202111010425.xz cnt/20211101/202111010426.xz cnt/20211101/202111010427.xz cnt/20211101/202111010428.xz cnt/20211101/202111010429.xz cnt/20211101/202111010430.xz cnt/20211101/202111010431.xz cnt/20211101/202111010432.xz cnt/20211101/202111010433.xz cnt/20211101/202111010434.xz cnt/20211101/202111010435.xz cnt/20211101/202111010436.xz cnt/20211101/202111010437.xz cnt/20211101/202111010438.xz cnt/20211101/202111010439.xz cnt/20211101/202111010440.xz cnt/20211101/202111010441.xz cnt/2021110_/usr/local/ripgrep-13.0.0-x86_64-unknown-linux-musl/rg_30282";
        arrs[1] = "10.125.126.01";
        arrs[2] = "ababababab";
        arrs[3] = "1234567890";
        arrs[4] = "1234567890abcdefghigklmnopqrstuvwxyz";
        arrs[5] = "1";
        arrs[6] = "(squid-1) -f /etc/squid/squid.conf";
        arrs[7] = "1af15f44-8b7f-4814-9a65-02b6c77d5309";

        for (String str : arrs) {
            System.out.print("raw string : " + str.getBytes().length);
            byte[] bytes1 = zlibCompress(str.getBytes());
            System.out.print(", compress string length : " + bytes1.length);
            byte[] bytes2 = zlibInfCompress(bytes1);
            String str2 = new String(bytes2);
            System.out.println(", equals : " + str2.equals(str));
        }

    }

}
