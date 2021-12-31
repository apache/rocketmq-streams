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
package org.apache.rocketmq.streams.common.cache.compress;

import java.util.Arrays;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.junit.Test;

/**
 * @author zengyu.cw
 * @program rocketmq-streams-apache
 * @create 2021-11-21 22:41:15
 * @description
 */
public class NumberUtilsTest {

    private void testFloat(float f) {
        byte[] b = NumberUtils.float2Bytes(f);
        float f1 = NumberUtils.bytes2Float(b);
        System.out.println("input = " + f + ", output = " + f1);

    }

    private void testInt(int i) {
        byte[] b = NumberUtils.toByte(i);
        int f1 = NumberUtils.toInt(b);
        System.out.println("input = " + i + ", output = " + f1);

    }

    private void testLong(long i) {
        byte[] b = NumberUtils.toByte(i);
        System.out.println(i + " array is " + Arrays.toString(b));
        long f1 = NumberUtils.toLong(b);
        System.out.println("input = " + i + ", output = " + f1);

    }

    @Test
    public void testInt() {
        int i = 123;
        testInt(i);
    }

    @Test
    public void testFloat() {
        float f1 = 3.2f;
        testFloat(f1);
    }

    @Test
    public void testLong() {
        long l1 = 16777173L;
        long l2 = 16777287L;
        testLong(l1);
        testLong(l2);
//        KVAddress kvAddress = new KVAddress(new ByteArray(NumberUtils.toByte(l2)));
        KVAddress kvAddress = KVAddress.createMapAddressFromLongValue(l2);
        System.out.println(kvAddress.convertLongValue());
    }

    @Test
    public void testString() {
        String str1 = "123";
        String str2 = "123 ";
        DataType datatype = DataTypeUtil.getDataType("string");
        System.out.println(Arrays.toString(str1.getBytes()));
        System.out.println(Arrays.toString(str2.getBytes()));
        System.out.println(Arrays.toString(datatype.toBytes(str1, true)));
        System.out.println(Arrays.toString(datatype.toBytes(str2, true)));

    }
}
