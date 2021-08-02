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
package org.apache.rocketmq.streams.script.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.junit.Test;

import java.util.List;

public class GrokFunctionTest {

    /**
     * 用grok做解析
     */
    @Test
    public void testGrok() {
        String logMsg = "fdsf 2019-05-06 09:26:07.874593+08:00 fdfd";
        String grok = "%{TIMESTAMP_ISO8601:timestamp}";
        JSONObject msg = new JSONObject();
        msg.put("data", logMsg);
        long start = System.currentTimeMillis();
        String scriptValue = "grok('data','" + grok + "');rm('data');";
        List<IMessage> list = ScriptComponent.getInstance().getService().executeScript(msg, scriptValue);
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i).getMessageBody());
        }
    }
}
