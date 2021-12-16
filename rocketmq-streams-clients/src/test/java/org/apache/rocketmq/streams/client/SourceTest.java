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
package org.apache.rocketmq.streams.client;

import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.junit.Test;

public class SourceTest {

    @Test
    public void testSource(){
        FileSource source = new FileSource("/tmp/file.txt");
        source.setJsonData(true);
        source.init();
        source.start(new IStreamOperator() {
            @Override
            public Object doMessage(IMessage message, AbstractContext context) {
                System.out.println(message.getMessageBody());
                return null;
            }
        });
    }


    @Test
    public void testImportMsgFromSource(){
        DataStreamSource.create("tmp","tmp")
            .fromRocketmq("TOPIC_AEGIS_DETECT_MSG","chris_test","T_MSG_PROC",true,null)
            .toFile("/tmp/aegis_proc.txt",true)
        .start();
    }

    @Test
    public void testImportMsgFromNet(){
        DataStreamSource.create("tmp","tmp")
            .fromRocketmq("TOPIC_AEGIS_DETECT_MSG","chris_test","T_MSG_NETSTAT",true,null)
            .toFile("/tmp/aegis_net.txt",true)
            .start();
    }
}
