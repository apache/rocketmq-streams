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

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.topology.IShuffleKeyGenerator;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.operator.impl.AggregationScript;
import org.apache.rocketmq.streams.window.WindowConstants;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.util.ShuffleUtil;

public class ShuffleMessageCache extends MessageCache<Pair<ISplit<?, ?>, IMessage>> {

    protected Map<String, MiniBatch> groupBy2WindowValue = new HashMap<>();

    protected transient IShuffleKeyGenerator shuffleKeyGenerator;
    protected transient AbstractShuffleWindow window;
    protected transient AtomicLong SUM = new AtomicLong(0);

    public ShuffleMessageCache(
        IMessageFlushCallBack<Pair<ISplit<?, ?>, IMessage>> flushCallBack) {
        super(flushCallBack);
    }

    @Override public synchronized int addCache(Pair<ISplit<?, ?>, IMessage> pair) {
        boolean openMiniBatch = isOpenMiniBatch();
        ISplit<?, ?> split = pair.getLeft();
        IMessage message = pair.getRight();
        if (openMiniBatch) {
            String groupByValue = shuffleKeyGenerator.generateShuffleKey(message);
            if (StringUtil.isEmpty(groupByValue)) {
                groupByValue = "<null>";
            }
            List<WindowInstance> windowInstances = (List<WindowInstance>) message.getMessageBody().get(WindowInstance.class.getSimpleName());
            if (windowInstances == null) {
                windowInstances = this.window.queryOrCreateWindowInstanceOnly(message, split.getQueueId());
            }
            for (WindowInstance windowInstance : windowInstances) {
                String key = MapKeyUtil.createKey(windowInstance.createWindowInstanceId(), groupByValue);
                MiniBatch miniBatch = groupBy2WindowValue.get(key);
                if (miniBatch == null) {
                    miniBatch = new MiniBatch();
                    groupBy2WindowValue.put(key, miniBatch);

                }
                IMessage newMergeMessage = miniBatch.calculate(this.window, message, groupByValue);
                if (newMergeMessage != null) {
                    pair.setValue(newMergeMessage);
                    return super.addCache(pair);
                }
            }
        } else {
            return super.addCache(pair);
        }

        return 0;
    }

    protected boolean isOpenMiniBatch() {
        if (!(window instanceof WindowOperator)) {
            return false;
        }
        if (window.isEmptyGroupBy() && WindowOperator.class.getSimpleName().equals(window.getClass().getSimpleName())) {
            return false;
        }
        return ComponentCreator.getPropertyBooleanValue(ConfigurationKey.WINDOW_MINI_BATCH_SWITCH);
    }

    protected JSONObject createMsg(String shuffleKey, WindowValue windowValue, MessageHeader messageHeader, JSONObject msgHeader) {

        JSONObject message = new JSONObject();
        message.put(WindowValue.class.getName(), windowValue);
        message.put(AggregationScript.INNER_AGGREGATION_COMPUTE_KEY, AggregationScript.INNER_AGGREGATION_COMPUTE_MULTI);
        IMessage windowValueMsg = new Message(message);
        windowValueMsg.setHeader(messageHeader);
        ShuffleUtil.createShuffleMsg(windowValueMsg, shuffleKey, msgHeader);

        if (windowValue.getcomputedResult() instanceof JSONObject) {
            message.putAll(windowValue.getcomputedResult());
        } else {
            Iterator<Map.Entry<String, Object>> it = windowValue.iteratorComputedColumnResult();
            while (it.hasNext()) {
                Map.Entry<String, Object> entry = it.next();
                message.put(entry.getKey(), entry.getValue());
            }
        }

        return message;

    }

    @Override public synchronized int flush() {
        this.groupBy2WindowValue = new HashMap<>();
        return super.flush();
    }

    public IShuffleKeyGenerator getShuffleKeyGenerator() {
        return shuffleKeyGenerator;
    }

    public void setShuffleKeyGenerator(
        IShuffleKeyGenerator shuffleKeyGenerator) {
        this.shuffleKeyGenerator = shuffleKeyGenerator;
    }

    public AbstractShuffleWindow getWindow() {
        return window;
    }

    public void setWindow(AbstractShuffleWindow window) {
        this.window = window;
    }

    protected class MiniBatch {
        protected WindowValue windowValue;
        protected IMessage message;

        public MiniBatch() {
            windowValue = new WindowValue();
        }

        public IMessage calculate(AbstractWindow window, IMessage msg, String groupByValue) {
            windowValue.calculate(window, msg);
            JSONObject mergeMsg = createMsg(groupByValue, windowValue, msg.getHeader(), msg.getMessageBody().getJSONObject(WindowConstants.ORIGIN_MESSAGE_HEADER));
            if (window.getTimeFieldName() != null) {
                mergeMsg.put(window.getTimeFieldName(), msg.getMessageBody().getString(window.getTimeFieldName()));
            }
            if (msg.getMessageBody().get(WindowInstance.class.getSimpleName()) != null) {
                mergeMsg.put(WindowInstance.class.getSimpleName(), msg.getMessageBody().get(WindowInstance.class.getSimpleName()));
            }
            if (msg.getMessageBody().get(AbstractWindow.class.getSimpleName()) != null) {
                mergeMsg.put(AbstractWindow.class.getSimpleName(), msg.getMessageBody().get(AbstractWindow.class.getSimpleName()));
            }

            if (this.message == null) {
                this.message = new Message(mergeMsg);
                return message;
            } else {
                this.message.getMessageBody().putAll(mergeMsg);
            }

            return null;
        }

    }
}
