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

import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.junit.Test;

/**
 * 先通过sink写入数据后，再测试消费
 */
public class SourceTest {
    @Test
    public void testSource() throws InterruptedException {
        ISource source = createSource();
        source.start((message, context) -> {
            if (message.getHeader().isSystemMessage()) {
                return null;
            }
            //处理消息,消息存储在messageBody中，所有消息都是json格式
            JSONObject msg = message.getMessageBody();
            System.out.println(msg);
            return null;
        });
        while (true) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void testSourceNotJson() throws InterruptedException {
        FileSource source = createSource();
        source.setJsonData(false);
        source.start((message, context) -> {
            if (message.getHeader().isSystemMessage()) {
                return null;
            }
            // 处理消息,消息存储在messageBody中， 如果消息不是json，在channel设置JsonData=false，系统会把原始消息放到data字段中
            Object oriMsg = ((UserDefinedMessage)message.getMessageBody()).getMessageValue();
            System.out.println(oriMsg);
            return null;
        });
        while (true) {
            Thread.sleep(1000);
        }
    }

    /**
     * 创建sink对象
     *
     * @return
     */
    protected FileSource createSource() {
        FileSource fileChannel = new FileSource("/Users/yuanxiaodong/chris/aegis_net_5000");
        fileChannel.init();
        return fileChannel;
    }
}
