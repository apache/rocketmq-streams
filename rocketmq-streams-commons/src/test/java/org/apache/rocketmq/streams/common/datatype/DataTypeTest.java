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
package org.apache.rocketmq.streams.common.datatype;

import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class DataTypeTest {
    @Test
    public void testDataType() {
        int num = 1233232;
        DataType datatype = DataTypeUtil.getDataTypeFromClass(Integer.class);
        assertTrue(datatype.matchClass(Integer.class) == true);
        String jsonStr = datatype.toDataJson(num);//把int序列化成字符串
        Integer value = (Integer)datatype.getData(jsonStr);//反序列化成int
        assertTrue(value == num);
        byte[] bytes = datatype.toBytes(num, true);//对数字压缩,序列化成字节数组
        assertTrue(bytes.length == 3);
        value = (Integer)datatype.byteToValue(bytes);//还原数据
        assertTrue(value == num);
    }
}
