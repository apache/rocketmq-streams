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
package org.apache.rocketmq.streams.common.channel.impl;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.PrintUtil;

import java.util.List;

/**
 * 测试使用，输出就是把消息打印出来
 */
public class OutputPrintChannel extends AbstractSink {

    private static int counter = 1;
    private transient boolean start = false;
    private static long startTime = System.currentTimeMillis();
    private static long begin = startTime;
    private static int step = 40000;

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        StringBuilder stringBuilder = new StringBuilder();
        for (IMessage msg : messages) {
            stringBuilder.append(msg.getMessageValue().toString() + PrintUtil.LINE);
        }
        System.out.println(stringBuilder.toString());
        return false;
    }

}
