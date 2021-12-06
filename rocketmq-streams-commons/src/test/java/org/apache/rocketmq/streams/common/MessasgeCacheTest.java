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
package org.apache.rocketmq.streams.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.channel.impl.OutputPrintChannel;
import org.apache.rocketmq.streams.common.context.Message;
import org.junit.Test;

public class MessasgeCacheTest {
    @Test
    public void testSink() throws InterruptedException {
        OutputPrintChannel first=new OutputPrintChannel();
        OutputPrintChannel second=new OutputPrintChannel();
        first.init();
        second.init();

        for(int i=0;i<20;i++){
            JSONObject msg=new JSONObject();
            msg.put("name","chris"+i);
            first.batchAdd(new Message(msg));
            JSONObject msg2=new JSONObject();
            msg2.put("name","chris2_"+i);
            second.batchAdd(new Message(msg2));
        }
        first.flush();
        second.flush();
        Thread.sleep(1000000000);
    }
}
