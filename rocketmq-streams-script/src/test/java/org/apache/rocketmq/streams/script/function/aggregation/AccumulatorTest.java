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
package org.apache.rocketmq.streams.script.function.aggregation;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.script.function.aggregation.AverageAccumulator.AverageAccum;
import org.apache.rocketmq.streams.script.function.aggregation.ConcatAccumulator.ConcatAccum;
import org.apache.rocketmq.streams.script.function.aggregation.ConcatDistinctAccumulator.ConcatDistinctAccum;
import org.apache.rocketmq.streams.script.function.aggregation.DistinctAccumulator.DistinctAccum;
import org.apache.rocketmq.streams.script.function.aggregation.MaxAccumulator.MaxAccum;
import org.apache.rocketmq.streams.script.function.aggregation.MinAccumulator.MinAccum;
import org.apache.rocketmq.streams.script.function.aggregation.SumAccumulator.SumAccum;
import org.junit.Assert;
import org.junit.Test;

public class AccumulatorTest {

    @Test
    public void testNumberStripTrailingZeros() {
        double value1 = 4.0;
        double value2 = 4.01;
        double value3 = 4.020;
        double value4 = 400;
        Number result1 = NumberUtils.stripTrailingZeros(value1);
        Number result2 = NumberUtils.stripTrailingZeros(value2);
        Number result3 = NumberUtils.stripTrailingZeros(value3);
        Number result4 = NumberUtils.stripTrailingZeros(value4);
        Assert.assertTrue(result1 instanceof Integer);
        Assert.assertTrue(result2 instanceof Double);
        Assert.assertTrue(result3 instanceof Double);
        Assert.assertTrue(result4 instanceof Integer);
    }

    @Test
    public void testAverageAccumulator() {
        AverageAccumulator averageAccumulator = new AverageAccumulator();
        AverageAccum averageAccum = averageAccumulator.createAccumulator();
        averageAccumulator.accumulate(averageAccum, 1);
        averageAccumulator.accumulate(averageAccum, 2.5);
        Assert.assertEquals(1.75, averageAccum.value);
        String objectValue = ReflectUtil.serializeObject(averageAccum).toJSONString();
        JSONObject objectJson = JSONObject.parseObject(objectValue);
        AverageAccum reflectAvgAccum = (AverageAccum)ReflectUtil.deserializeObject(objectJson);
        Assert.assertEquals(1.75, reflectAvgAccum.value);

        averageAccum = averageAccumulator.createAccumulator();
        averageAccumulator.accumulate(averageAccum, 1);
        averageAccumulator.accumulate(averageAccum, 2);
        objectValue = ReflectUtil.serializeObject(averageAccum).toJSONString();
        objectJson = JSONObject.parseObject(objectValue);
        reflectAvgAccum = (AverageAccum)ReflectUtil.deserializeObject(objectJson);
        Assert.assertEquals(1.5, reflectAvgAccum.value);

        averageAccum = averageAccumulator.createAccumulator();
        averageAccumulator.accumulate(averageAccum, 2);
        averageAccumulator.accumulate(averageAccum, 2);
        objectValue = ReflectUtil.serializeObject(averageAccum).toJSONString();
        objectJson = JSONObject.parseObject(objectValue);
        reflectAvgAccum = (AverageAccum)ReflectUtil.deserializeObject(objectJson);
        Assert.assertEquals(2, reflectAvgAccum.value);

        averageAccum = averageAccumulator.createAccumulator();
        averageAccumulator.accumulate(averageAccum, 2.0);
        averageAccumulator.accumulate(averageAccum, 2);
        Assert.assertEquals(2, averageAccum.value);
        objectValue = ReflectUtil.serializeObject(averageAccum).toJSONString();
        objectJson = JSONObject.parseObject(objectValue);
        reflectAvgAccum = (AverageAccum)ReflectUtil.deserializeObject(objectJson);
        Assert.assertEquals(2, reflectAvgAccum.value);
    }

    @Test
    public void testSumAccumulator() {
        SumAccumulator sumAccumulator = new SumAccumulator();
        SumAccum sumAccum = sumAccumulator.createAccumulator();
        sumAccumulator.accumulate(sumAccum, 1);
        sumAccumulator.accumulate(sumAccum, 2.5);
        String dbValue = ReflectUtil.serializeObject(sumAccum).toJSONString();
        JSONObject memObject = JSONObject.parseObject(dbValue);
        SumAccum reflectSumAccum = (SumAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals(3.5, reflectSumAccum.sum);

        sumAccum = sumAccumulator.createAccumulator();
        sumAccumulator.accumulate(sumAccum, 1);
        sumAccumulator.accumulate(sumAccum, 2);
        dbValue = ReflectUtil.serializeObject(sumAccum).toJSONString();
        memObject = JSONObject.parseObject(dbValue);
        reflectSumAccum = (SumAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals(3, reflectSumAccum.sum);
    }

    @Test
    public void testMinAccumulator() {
        MinAccumulator minAccumulator = new MinAccumulator();
        MinAccum minAccum = minAccumulator.createAccumulator();
        minAccumulator.accumulate(minAccum, 13);
        minAccumulator.accumulate(minAccum, 12);
        String dbAccum = ReflectUtil.serializeObject(minAccum).toJSONString();
        JSONObject memObject = JSONObject.parseObject(dbAccum);
        MinAccum reflectMinAccum = (MinAccum)ReflectUtil.deserializeObject(memObject);
        //TODO 序列化过程中的value都被转成了string 如果碰到这个问题 暂时在sql里加一个类型转换解决
        Assert.assertEquals("12", reflectMinAccum.min);

        minAccum = minAccumulator.createAccumulator();
        minAccumulator.accumulate(minAccum, "2021-06-16 12:00:00");
        minAccumulator.accumulate(minAccum, "2021-06-18 12:00:00");
        dbAccum = ReflectUtil.serializeObject(minAccum).toJSONString();
        memObject = JSONObject.parseObject(dbAccum);
        reflectMinAccum = (MinAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("2021-06-16 12:00:00", reflectMinAccum.min);

        minAccum = minAccumulator.createAccumulator();
        minAccumulator.accumulate(minAccum, "127.0.0.1");
        minAccumulator.accumulate(minAccum, "127.0.0.2");
        dbAccum = ReflectUtil.serializeObject(minAccum).toJSONString();
        memObject = JSONObject.parseObject(dbAccum);
        reflectMinAccum = (MinAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("127.0.0.1", reflectMinAccum.min);
    }

    @Test
    public void testMaxAccumulator() {
        MaxAccumulator maxAccumulator = new MaxAccumulator();
        MaxAccum maxAccum = maxAccumulator.createAccumulator();
        maxAccumulator.accumulate(maxAccum, 13);
        maxAccumulator.accumulate(maxAccum, 12);
        String dbAccum = ReflectUtil.serializeObject(maxAccum).toJSONString();
        JSONObject memObject = JSONObject.parseObject(dbAccum);
        MaxAccum reflectMaxAccum = (MaxAccum)ReflectUtil.deserializeObject(memObject);
        //TODO 序列化过程中的value都被转成了string 如果碰到这个问题 暂时在sql里加一个类型转换解决
        Assert.assertEquals("13", reflectMaxAccum.max);

        maxAccum = maxAccumulator.createAccumulator();
        maxAccumulator.accumulate(maxAccum, "2021-06-16 12:00:00");
        maxAccumulator.accumulate(maxAccum, "2021-06-18 12:00:00");
        dbAccum = ReflectUtil.serializeObject(maxAccum).toJSONString();
        memObject = JSONObject.parseObject(dbAccum);
        reflectMaxAccum = (MaxAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("2021-06-18 12:00:00", reflectMaxAccum.max);

        maxAccum = maxAccumulator.createAccumulator();
        maxAccumulator.accumulate(maxAccum, "127.0.0.1");
        maxAccumulator.accumulate(maxAccum, "127.0.0.2");
        dbAccum = ReflectUtil.serializeObject(maxAccum).toJSONString();
        memObject = JSONObject.parseObject(dbAccum);
        reflectMaxAccum = (MaxAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("127.0.0.2", reflectMaxAccum.max);
    }

    @Test
    public void testConcatAccumulator() {
        ConcatAccumulator concatAccumulator = new ConcatAccumulator();
        ConcatAccum concatAccum = concatAccumulator.createAccumulator();
        concatAccumulator.accumulate(concatAccum, "be");
        concatAccumulator.accumulate(concatAccum, "a");
        concatAccumulator.accumulate(concatAccum, "listener");
        String dbValue = ReflectUtil.serializeObject(concatAccum).toJSONString();
        JSONObject memObject = JSONObject.parseObject(dbValue);
        ConcatAccum reflectConcatAccum = (ConcatAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("be,a,listener", concatAccumulator.getValue(reflectConcatAccum));

        concatAccum = concatAccumulator.createAccumulator();
        concatAccumulator.accumulate(concatAccum, " ", "be");
        concatAccumulator.accumulate(concatAccum, " ", "a");
        concatAccumulator.accumulate(concatAccum, " ", "listener");
        dbValue = ReflectUtil.serializeObject(concatAccum).toJSONString();
        memObject = JSONObject.parseObject(dbValue);
        reflectConcatAccum = (ConcatAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("be a listener", concatAccumulator.getValue(reflectConcatAccum));
    }

    @Test
    public void testConcatDistinctAccumulator() {
        ConcatDistinctAccumulator concatDistinctAccumulator = new ConcatDistinctAccumulator();
        ConcatDistinctAccum concatDistinctAccum = concatDistinctAccumulator.createAccumulator();
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "talk");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "is");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "cheap");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "show");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "me");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "show");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "code");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, "code");
        String dbValue = ReflectUtil.serializeObject(concatDistinctAccum).toJSONString();
        JSONObject memObject = JSONObject.parseObject(dbValue);
        ConcatDistinctAccum reflectConcatAccum = (ConcatDistinctAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("code,show,me,talk,is,cheap", concatDistinctAccumulator.getValue(reflectConcatAccum));

        concatDistinctAccum = concatDistinctAccumulator.createAccumulator();
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "talk");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "is");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "cheap");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "show");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "me");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "show");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "code");
        concatDistinctAccumulator.accumulate(concatDistinctAccum, " ", "code");
        dbValue = ReflectUtil.serializeObject(concatDistinctAccum).toJSONString();
        memObject = JSONObject.parseObject(dbValue);
        reflectConcatAccum = (ConcatDistinctAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals("code show me talk is cheap", concatDistinctAccumulator.getValue(reflectConcatAccum));
    }

    @Test
    public void testDistinctAccumulator() {
        DistinctAccumulator distinctAccumulator = new DistinctAccumulator();
        DistinctAccum distinctAccum = distinctAccumulator.createAccumulator();
        distinctAccumulator.accumulate(distinctAccum, "pua");
        distinctAccumulator.accumulate(distinctAccum, "uap");
        distinctAccumulator.accumulate(distinctAccum, "apu");
        distinctAccumulator.accumulate(distinctAccum, "pua");
        String dbValue = ReflectUtil.serializeObject(distinctAccum).toJSONString();
        JSONObject memObject = JSONObject.parseObject(dbValue);
        DistinctAccum reflectDistinctAccum = (DistinctAccum)ReflectUtil.deserializeObject(memObject);
        Assert.assertEquals(3, distinctAccumulator.getValue(reflectDistinctAccum).size());
    }

}
