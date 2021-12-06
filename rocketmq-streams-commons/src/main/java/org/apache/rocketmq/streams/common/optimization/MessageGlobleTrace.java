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
package org.apache.rocketmq.streams.common.optimization;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageHeader;

/**
 * 判断pipeline是否完成一个分支的执行，如果未完成，可以配合LogFingerprintFilter做指纹过滤。某个指纹如果确定不触发某个规则，直接丢弃、 在pipeline中消息会被拆分，在有多分支时，会被copy，这个对象会在任何变动时，都保持全局唯一，不允许copy，复制，创建，一个message全局唯一
 */
public class MessageGlobleTrace {
    protected boolean existFinishBranch = false;//存在有完成的分支

    private MessageGlobleTrace() {

    }
    /**
     * 是否有完成的分支
     *
     * @param message
     * @return
     */
    public static Boolean existFinishBranch(IMessage message) {
        MessageGlobleTrace trace = message.getHeader().getMessageGloableTrace();
        if (trace == null) {
            return null;
        }
        return trace.existFinishBranch;
    }

    /**
     * 如果pipeline执行完了，则调用这个方法告诉追踪器
     *
     * @param message
     */
    public static void finishPipeline(IMessage message) {
        MessageGlobleTrace trace = message.getHeader().getMessageGloableTrace();
        if (trace == null) {
            return;
        }
        trace.existFinishBranch = true;
    }

    /**
     * 把一个message，关联一个全局追踪器，非必须
     *
     * @param message
     * @return
     */
    public static MessageGlobleTrace joinMessage(IMessage message) {
        MessageHeader messageHeader = message.getHeader();
        if (messageHeader.getMessageGloableTrace() == null) {
            messageHeader.setMessageGloableTrace(new MessageGlobleTrace());
        }
        return messageHeader.getMessageGloableTrace();
    }

    public static void clear(IMessage message) {
        MessageHeader messageHeader = message.getHeader();
        if (messageHeader.getMessageGloableTrace() == null) {
            messageHeader.setMessageGloableTrace(new MessageGlobleTrace());
        }
        messageHeader.getMessageGloableTrace().existFinishBranch = false;
    }
}
