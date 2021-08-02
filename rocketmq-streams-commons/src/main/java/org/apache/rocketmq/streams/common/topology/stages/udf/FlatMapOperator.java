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
package org.apache.rocketmq.streams.common.topology.stages.udf;

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;

import java.util.ArrayList;
import java.util.List;

/**
 * 自定义flat map
 */
public abstract class FlatMapOperator extends StageBuilder {
    public static class Collector {
        private List<IMessage> messages = new ArrayList<>();

        public void collect(IMessage message) {
            messages.add(message);
        }

    }

    @Override
    protected IMessage operate(IMessage message, AbstractContext context) {
        boolean needFlush = message.getHeader().isNeedFlush();
        context.openSplitModel();
        Collector collector = new Collector();
        context.openSplitModel();
        flatMap(message, context, collector);
        context.removeSpliteMessage(message);
        int i = 0;
        for (IMessage msg : collector.messages) {
            if (i < collector.messages.size() - 1) {
                msg.getHeader().setNeedFlush(false);
            } else {
                msg.getHeader().setNeedFlush(needFlush);
            }
            context.addSplitMessages(msg);
            i++;
        }

        return message;
    }

    protected abstract void flatMap(IMessage message, AbstractContext context, Collector collector);
}
