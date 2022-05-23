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
package org.apache.rocketmq.streams.common.channel;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.context.Message;
import org.junit.Test;

/**
 * 写入数据后，可以通过source读取验证是否写入成功
 */
public class SinkTest {

    @Test
    public void testSink() {
        ISink sink = createSink();
        sink.setBatchSize(100);//最大缓存条数
        for (int i = 0; i < 100; i++) {
            JSONObject msg = new JSONObject();
            msg.put("name", "chris");
            msg.put("age", 18);
            msg.put("msgId", i);
            sink.batchAdd(new Message(msg), null);//插入缓存，当条数>batchSize时，刷新存储
        }
        sink.flush();//把缓存当数据刷新到存储
    }

    @Test
    public void testSinkAutoFlush() throws InterruptedException {
        ISink sink = createSink();
        sink.setBatchSize(100);//最大缓存条数
        // sink.setActivtyTimeOut(1000);
        sink.openAutoFlush();//系统会自动刷新，刷新条件是间隔activtyTimeOut或条数大于batchSize
        for (int i = 0; i < 100; i++) {
            JSONObject msg = new JSONObject();
            msg.put("name", "chris");
            msg.put("age", 18);
            msg.put("msgId", i);
            sink.batchAdd(new Message(msg), null);//插入缓存，当条数>batchSize时，刷新存储
        }
        while (true) {
            Thread.sleep(1000);
        }
    }

    /**
     * 创建sink对象
     *
     * @return
     */
    protected ISink createSink() {
        FileSink fileChannel = new FileSink("/tmp/filename.txt");
        fileChannel.init();
        return fileChannel;
    }
}
