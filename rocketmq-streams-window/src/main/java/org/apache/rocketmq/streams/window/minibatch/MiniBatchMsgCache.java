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
package org.apache.rocketmq.streams.window.minibatch;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.AbstractMultiSplitMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.shuffle.IShuffleKeyGenerator;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;

public class MiniBatchMsgCache extends AbstractMultiSplitMessageCache<Pair<ISplit,IMessage>> {
    public static String SHUFFLE_KEY="shuffle_key";



    protected transient IShuffleKeyGenerator shuffleKeyGenerator;
    protected transient AbstractShuffleWindow window;




    public MiniBatchMsgCache(
        IMessageFlushCallBack<Pair<ISplit,IMessage>> flushCallBack, IShuffleKeyGenerator shuffleKeyGenerator,
        AbstractShuffleWindow window) {
        super(flushCallBack);
        this.shuffleKeyGenerator=shuffleKeyGenerator;
        this.window=window;
    }


    @Override protected String createSplitId(Pair<ISplit, IMessage> msg) {
        return msg.getLeft().getQueueId();
    }

    @Override protected MessageCache createMessageCache() {
        ShuffleMessageCache messageCache=new ShuffleMessageCache(this.flushCallBack);
        messageCache.setWindow(window);
        messageCache.setShuffleKeyGenerator(shuffleKeyGenerator);
        return messageCache;
    }
}
