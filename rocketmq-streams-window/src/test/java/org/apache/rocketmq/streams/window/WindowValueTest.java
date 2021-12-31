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
package org.apache.rocketmq.streams.window;

import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.apache.rocketmq.streams.script.function.aggregation.CountAccumulator;
import org.apache.rocketmq.streams.script.function.aggregation.SumAccumulator;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class WindowValueTest {
    /**
     * 100w(asset):47s
     * 100w(no asset):
     */
    @Test
    public void testBatchInsert() {
        long start = System.currentTimeMillis();
        for (int i = 0; i < 10; i++) {
            WindowValue windowValue = new WindowValue();
            windowValue.setId(1111111L);
            windowValue.setGroupBy("fdsdf");
            Map<String, Object> map = new HashMap<>();
            CountAccumulator.CountAccum countAccum = new CountAccumulator.CountAccum();
            countAccum.count = 1;
            map.put("count", countAccum);
            SumAccumulator.SumAccum sumAccum = new SumAccumulator.SumAccum();
            sumAccum.sum = 10;
            map.put("sum", sumAccum);
            windowValue.setAggColumnMap(map);
            windowValue.setPartitionNum(434433443);
            windowValue.setPartition("fdsfdsdf");
            byte[] bytes = SerializeUtil.serialize(windowValue);
            windowValue = SerializeUtil.deserialize(bytes);
            countAccum = (CountAccumulator.CountAccum) windowValue.getAggColumnResultByKey("count");
            assertTrue(countAccum.count == 1);

            sumAccum = (SumAccumulator.SumAccum) windowValue.getAggColumnResultByKey("sum");
            assertTrue(sumAccum.sum.equals(10));
            assertTrue(windowValue.getGroupBy().equals("fdsdf") && windowValue.getPartition().equals("fdsfdsdf") && windowValue.getPartitionNum() == 434433443);

        }
        System.out.println("finish cost is  " + (System.currentTimeMillis() - start));
    }

    @Test
    public void testS() {
        String value = "W3sicmVzdWx0IjoiVlFCdmNtY3VZWEJoWTJobExuSnZZMnRsZEcxeExuTjBjbVZoYlhNdWMyTnlhWEIwTG1aMWJtTjBhVzl1TG1GblozSmxaMkYwYVc5dUxrTnZibU5oZEVGalkzVnRkV3hoZEc5eUpFTnZibU5oZEVGalkzVnRBUUFHQUNkY2NseHVKellBV3lJeU1ESXhMVEE1TFRFMUlERTFPak13T2pVMklDMGdjR2x1WnlBdGJpQXhJQzEzSURJd01EQWdNVEF1TkRJdU1DNHhOaUpkIiwiZGF0YXR5cGUiOiJqYXZhQmVhbiIsImZ1bmN0aW9uIjoic3ViX3Byb2NfZGV0YWlsXzEiLCJpc0Jhc2ljIjp0cnVlfSx7InJlc3VsdCI6IlR3QnZjbWN1WVhCaFkyaGxMbkp2WTJ0bGRHMXhMbk4wY21WaGJYTXVjMk55YVhCMExtWjFibU4wYVc5dUxtRm5aM0psWjJGMGFXOXVMazFoZUVGalkzVnRkV3hoZEc5eUpFMWhlRUZqWTNWdEFRQVdBRU02TDFCNWRHaHZiakkzTDNCNWRHaHZiaTVsZUdVPSIsImRhdGF0eXBlIjoiamF2YUJlYW4iLCJmdW5jdGlvbiI6InBwZXhlXzAiLCJpc0Jhc2ljIjp0cnVlfSx7InJlc3VsdCI6IlR3QnZjbWN1WVhCaFkyaGxMbkp2WTJ0bGRHMXhMbk4wY21WaGJYTXVjMk55YVhCMExtWjFibU4wYVc5dUxtRm5aM0psWjJGMGFXOXVMazFoZUVGalkzVnRkV3hoZEc5eUpFMWhlRUZqWTNWdEFRQVRBREl3TWpFdE1Ea3RNVFVnTVRVNk16QTZOVFk9IiwiZGF0YXR5cGUiOiJqYXZhQmVhbiIsImZ1bmN0aW9uIjoibWF4X3RpbWVfMCIsImlzQmFzaWMiOnRydWV9LHsicmVzdWx0IjoiVHdCdmNtY3VZWEJoWTJobExuSnZZMnRsZEcxeExuTjBjbVZoYlhNdWMyTnlhWEIwTG1aMWJtTjBhVzl1TG1GblozSmxaMkYwYVc5dUxrMWhlRUZqWTNWdGRXeGhkRzl5SkUxaGVFRmpZM1Z0QVFBa0FEZGlNR1V6WmpRNExUYzBNREV0TkRrNU5DMWlNMlF6TFRRMVlqVmlaamRqWkRBd05RPT0iLCJkYXRhdHlwZSI6ImphdmFCZWFuIiwiZnVuY3Rpb24iOiJ1dWlkXzAiLCJpc0Jhc2ljIjp0cnVlfSx7InJlc3VsdCI6IlZRQnZjbWN1WVhCaFkyaGxMbkp2WTJ0bGRHMXhMbk4wY21WaGJYTXVjMk55YVhCMExtWjFibU4wYVc5dUxtRm5aM0psWjJGMGFXOXVMa052Ym1OaGRFRmpZM1Z0ZFd4aGRHOXlKRU52Ym1OaGRFRmpZM1Z0QVFBREFDY3ZKd2dBV3lKd2FXNW5JbDA9IiwiZGF0YXR5cGUiOiJqYXZhQmVhbiIsImZ1bmN0aW9uIjoic3ViX3Byb2NfYWxsXzAiLCJpc0Jhc2ljIjp0cnVlfSx7InJlc3VsdCI6IlR3QnZjbWN1WVhCaFkyaGxMbkp2WTJ0bGRHMXhMbk4wY21WaGJYTXVjMk55YVhCMExtWjFibU4wYVc5dUxtRm5aM0psWjJGMGFXOXVMazFoZUVGalkzVnRkV3hoZEc5eUpFMWhlRUZqWTNWdEFRQUVBREl6TWpnPSIsImRhdGF0eXBlIjoiamF2YUJlYW4iLCJmdW5jdGlvbiI6InBwaWRfMCIsImlzQmFzaWMiOnRydWV9LHsicmVzdWx0IjoiVXdCdmNtY3VZWEJoWTJobExuSnZZMnRsZEcxeExuTjBjbVZoYlhNdWMyTnlhWEIwTG1aMWJtTjBhVzl1TG1GblozSmxaMkYwYVc5dUxrTnZkVzUwUVdOamRXMTFiR0YwYjNJa1EyOTFiblJCWTJOMWJRRUFBUUFBQUE9PSIsImRhdGF0eXBlIjoiamF2YUJlYW4iLCJmdW5jdGlvbiI6ImNtZF9jbnRfMCIsImlzQmFzaWMiOnRydWV9LHsicmVzdWx0IjoiV1FCdmNtY3VZWEJoWTJobExuSnZZMnRsZEcxeExuTjBjbVZoYlhNdWMyTnlhWEIwTG1aMWJtTjBhVzl1TG1GblozSmxaMkYwYVc5dUxrUnBjM1JwYm1OMFFXTmpkVzExYkdGMGIzSWtSR2x6ZEdsdVkzUkJZMk4xYlFFQUFRQUVBSEJwYm1jPSIsImRhdGF0eXBlIjoiamF2YUJlYW4iLCJmdW5jdGlvbiI6InN1Yl9wcm9jX2NudF8wIiwiaXNCYXNpYyI6dHJ1ZX0seyJyZXN1bHQiOiJVd0J2Y21jdVlYQmhZMmhsTG5KdlkydGxkRzF4TG5OMGNtVmhiWE11YzJOeWFYQjBMbVoxYm1OMGFXOXVMbUZuWjNKbFoyRjBhVzl1TGtOdmRXNTBRV05qZFcxMWJHRjBiM0lrUTI5MWJuUkJZMk4xYlFFQUFRQUFBQT09IiwiZGF0YXR5cGUiOiJqYXZhQmVhbiIsImZ1bmN0aW9uIjoic3ViX3Byb2NfY250XzEiLCJpc0Jhc2ljIjp0cnVlfSx7InJlc3VsdCI6IlR3QnZjbWN1WVhCaFkyaGxMbkp2WTJ0bGRHMXhMbk4wY21WaGJYTXVjMk55YVhCMExtWjFibU4wYVc5dUxtRm5aM0psWjJGMGFXOXVMazFoZUVGalkzVnRkV3hoZEc5eUpFMWhlRUZqWTNWdEFRQUtBSEI1ZEdodmJpNWxlR1U9IiwiZGF0YXR5cGUiOiJqYXZhQmVhbiIsImZ1bmN0aW9uIjoicHByb2NfbmFtZV8wIiwiaXNCYXNpYyI6dHJ1ZX0seyJyZXN1bHQiOiJUd0J2Y21jdVlYQmhZMmhsTG5KdlkydGxkRzF4TG5OMGNtVmhiWE11YzJOeWFYQjBMbVoxYm1OMGFXOXVMbUZuWjNKbFoyRjBhVzl1TGsxcGJrRmpZM1Z0ZFd4aGRHOXlKRTFwYmtGalkzVnRBUUFUQURJd01qRXRNRGt0TVRVZ01UVTZNekE2TlRZPSIsImRhdGF0eXBlIjoiamF2YUJlYW4iLCJmdW5jdGlvbiI6Im1pbl90aW1lXzAiLCJpc0Jhc2ljIjp0cnVlfSx7InJlc3VsdCI6IlR3QnZjbWN1WVhCaFkyaGxMbkp2WTJ0bGRHMXhMbk4wY21WaGJYTXVjMk55YVhCMExtWjFibU4wYVc5dUxtRm5aM0psWjJGMGFXOXVMazFoZUVGalkzVnRkV3hoZEc5eUpFMWhlRUZqWTNWdEFRQXlBSEI1ZEdodmJpQWdZenBjYzJOeWFYQjBjMXh3YVc1bkxYSjBMWE4wTFd4aGJpMXBjQzV3ZVNBeE1DNDBNaTR3TGpFMiIsImRhdGF0eXBlIjoiamF2YUJlYW4iLCJmdW5jdGlvbiI6InBjbWRfMCIsImlzQmFzaWMiOnRydWV9XQ==";
        WindowValue windowValue = new WindowValue();
        ReflectUtil.setBeanFieldValue(windowValue, "aggColumnResult", value);
    }
}
