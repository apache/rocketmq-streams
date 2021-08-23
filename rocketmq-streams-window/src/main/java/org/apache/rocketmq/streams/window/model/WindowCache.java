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
package org.apache.rocketmq.streams.window.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.shuffle.ShuffleChannel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 缓存数据，flush时，刷新完成数据落盘
 */
public abstract class WindowCache extends
    AbstractSink {

    private static final Log LOG = LogFactory.getLog(WindowCache.class);

    public static final String SPLIT_SIGN = "##";

    public static final String ORIGIN_OFFSET = "origin_offset";

    public static final String ORIGIN_QUEUE_ID = "origin_queue_id";


    public static final String ORIGIN_QUEUE_IS_LONG = "origin_offset_is_LONG";

    public static final String ORIGIN_MESSAGE_HEADER = "origin_message_header";


    public static final String ORIGIN_SOURCE_NAME="origin_offset_name";

    public static final String SHUFFLE_KEY = "SHUFFLE_KEY";

    public static final String ORIGIN_MESSAGE_TRACE_ID = "origin_request_id";

    /**
     * 分片转发channel
     */
    protected transient ShuffleChannel shuffleChannel;

    @Override
    protected boolean batchInsert(List<IMessage> messageList) {
        Map<Integer, JSONArray> shuffleMap = translateToShuffleMap(messageList);
        if (shuffleMap != null && shuffleMap.size() > 0) {
            Set<String> splitIds=new HashSet<>();

            for (Map.Entry<Integer, JSONArray> entry : shuffleMap.entrySet()) {
                ISplit split=shuffleChannel.getSplit(entry.getKey());
                JSONObject msg=shuffleChannel.createMsg(entry.getValue(),split);
                shuffleChannel.getProducer().batchAdd(new Message(msg),split);
                splitIds.add(split.getQueueId());
                List<IMessage> messages=new ArrayList<>();


                if(DebugWriter.getDebugWriter(shuffleChannel.getWindow().getConfigureName()).isOpenDebug()){
                    JSONArray jsonArray=entry.getValue();
                    for(int i=0;i<jsonArray.size();i++){
                        messages.add(new Message(jsonArray.getJSONObject(i)));
                    }
                    DebugWriter.getDebugWriter(shuffleChannel.getWindow().getConfigureName()).writeWindowCache(shuffleChannel.getWindow(),messages,split.getQueueId());
                }

            }

            shuffleChannel.getProducer().flush(splitIds);
        }
        return true;
    }

    /**
     * 对接收的消息按照不同shuffle key进行分组
     *
     * @param messages
     * @return
     */
    protected Map<Integer, JSONArray> translateToShuffleMap(List<IMessage> messages) {
        Map<Integer, JSONArray> shuffleMap = new HashMap<>();
        for (IMessage msg : messages) {
            if (msg.getHeader().isSystemMessage()) {
                continue;
            }
            String shuffleKey = generateShuffleKey(msg);
            if (StringUtil.isEmpty(shuffleKey)) {
                shuffleKey="<null>";
                LOG.debug("there is no group by value in message! " + msg.getMessageBody().toString());
                //continue;
            }
            Integer index = shuffleChannel.hash(shuffleKey);
            JSONObject body = msg.getMessageBody();
            String offset = msg.getHeader().getOffset();
            String queueId = msg.getHeader().getQueueId();

            body.put(ORIGIN_OFFSET, offset);
            body.put(ORIGIN_QUEUE_ID, queueId);
            body.put(ORIGIN_QUEUE_IS_LONG,msg.getHeader().getMessageOffset().isLongOfMainOffset());
            body.put(ORIGIN_MESSAGE_HEADER, JSONObject.toJSONString(msg.getHeader()));
            body.put(ORIGIN_MESSAGE_TRACE_ID, msg.getHeader().getTraceId());
            body.put(SHUFFLE_KEY, shuffleKey);

            addPropertyToMessage(msg, body);

            JSONArray jsonArray = shuffleMap.get(index);
            if (jsonArray == null) {
                jsonArray = new JSONArray();
                shuffleMap.put(index, jsonArray);
            }
            jsonArray.add(body);

        }
        return shuffleMap;
    }



    /**
     * 根据message生成shuffle key
     *
     * @param message
     * @return
     */
    protected abstract String generateShuffleKey(IMessage message);

    /**
     * 如果需要额外的字段附加到shuffle前的message，通过实现这个子类增加
     *
     * @param oriJson
     */
    protected void addPropertyToMessage(IMessage oriMessage, JSONObject oriJson){

    }

    public ShuffleChannel getShuffleChannel() {
        return shuffleChannel;
    }

    public void setShuffleChannel(ShuffleChannel shuffleChannel) {
        this.shuffleChannel = shuffleChannel;
    }
}
