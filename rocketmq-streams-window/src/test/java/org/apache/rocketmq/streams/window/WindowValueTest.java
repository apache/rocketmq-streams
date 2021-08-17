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

import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.junit.Test;

public class WindowValueTest {

    @Test
    public void testBatchInsert() {
        WindowValue windowValue = new WindowValue();
        windowValue.setId(1111111L);
        windowValue.setGroupBy("fdsdf");

        WindowValue windowValue2 = new WindowValue();

        windowValue.setGroupBy("2");
        ORMUtil.batchReplaceInto(windowValue, windowValue2);
    }
}
