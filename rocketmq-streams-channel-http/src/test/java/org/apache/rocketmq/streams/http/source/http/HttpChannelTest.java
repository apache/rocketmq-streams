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
package org.apache.rocketmq.streams.http.source.http;

import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.utils.ThreadUtil;
import org.apache.rocketmq.streams.http.source.HttpSource;
import org.junit.Test;

public class HttpChannelTest {

    @Test
    public void startHttpChannel() {
        ISource channel = create();
        channel.start((message, context) -> {
            System.out.println("receive message " + message.getMessageBody());
            return message;
        });
        while (true) {
            ThreadUtil.sleep(1000);
        }
    }

    private ISource create() {
        HttpSource httpChannel = new HttpSource();
        httpChannel.addPath("/chris/yuanxiaodong");
        httpChannel.setHttps(false);
        httpChannel.init();
        return httpChannel;
    }
}