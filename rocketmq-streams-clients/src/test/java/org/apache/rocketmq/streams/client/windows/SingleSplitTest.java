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
package org.apache.rocketmq.streams.client.windows;

import org.apache.rocketmq.streams.client.StreamExecutionEnvironment;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.junit.Test;

public class SingleSplitTest extends AbstractWindowTest {

    protected DataStream createSourceDataStream() {
        return StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name1")
            .fromFile(filePath, true);
    }

    @Test
    public void testFile() {
        StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name1")
            .fromFile(filePath, false)
            .map(new MapFunction<String, String>() {

                @Override public String map(String message) throws Exception {
                    System.out.println(message);
                    return message;
                }
            })
            .toPrint().start();
    }

    protected int getSourceCount() {
        return 10;
    }

    /**
     * validate the window result  meet expectations
     */
    @Test
    public void testWindowResult() {
        super.testWindowResult(getSourceCount());
    }

    /**
     * @throws InterruptedException
     */
    @Test
    public void testFireMode0() throws InterruptedException {
        // ComponentCreator.getProperties().setProperty("window.fire.isTest","true");
//        ComponentCreator.getProperties().setProperty("dipper.configurable.polling.time", "-1");
        super.executeWindowStream(true, 5, IWindow.DEFAULTFIRE_MODE, 0, 200l);
    }

    @Test
    public void testFireMode1() throws InterruptedException {
        super.executeWindowStream(false, 5, IWindow.MULTI_WINDOW_INSTANCE_MODE, 0, 20l);
    }

    @Test
    public void testMutilWindow() {
        super.testMutilWindow(false);
    }

}
