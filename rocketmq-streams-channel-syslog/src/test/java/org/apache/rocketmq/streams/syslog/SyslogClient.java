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
package org.apache.rocketmq.streams.syslog;

import com.alibaba.fastjson.JSONObject;
import java.util.Date;
import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IPUtil;
import org.junit.Test;

public class SyslogClient {

    @Test
    public void sendSyslog() throws InterruptedException {
        SyslogChannel syslogChannel = createSyslogChannel();

        syslogChannel.start(new IStreamOperator() {
            @Override
            public Object doMessage(IMessage message, AbstractContext context) {
                System.out.println(message.getMessageBody());
                return null;
            }
        });
        addData(syslogChannel);
        Thread.sleep(1000000000l);
    }

    private void addData(IChannel channel) {
        JSONObject msg = new JSONObject();
        msg.put("name", "chris");
        //msg.put("host",IPUtil.getLocalIP());
        channel.batchAdd(new Message(msg));
        channel.flush();
    }

    private SyslogChannel createSyslogChannel() {
        SyslogChannel syslogChannel = new SyslogChannel();
        syslogChannel.setUDPProtol();
        syslogChannel.addIps(IPUtil.getLocalIP());
        syslogChannel.setServerIp("11.158.144.159");
        syslogChannel.init();
        return syslogChannel;
    }

    @Test
    public void testDate() {
        Date date = new Date();
        String result = DateUtil.format(date, "dd hh:mm:ss");
        System.out.println(result);
    }
}
