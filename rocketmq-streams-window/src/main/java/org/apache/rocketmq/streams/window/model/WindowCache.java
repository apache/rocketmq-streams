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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.topology.model.IWindow;
import org.apache.rocketmq.streams.common.topology.shuffle.IShuffleKeyGenerator;
import org.apache.rocketmq.streams.common.utils.CompressUtil;
import org.apache.rocketmq.streams.window.minibatch.MiniBatchMsgCache;
import org.apache.rocketmq.streams.window.shuffle.ShuffleChannel;
import org.apache.rocketmq.streams.window.util.ShuffleUtil;

/**
 * 缓存数据，flush时，刷新完成数据落盘
 */
public abstract class WindowCache extends
    AbstractSink implements IWindow.IWindowCheckpoint {

    private static final Log LOG = LogFactory.getLog(WindowCache.class);

    public static final String SPLIT_SIGN = "##";

    public static final String IS_COMPRESSION_MSG = "_is_compress_msg";
    public static final String COMPRESSION_MSG_DATA = "_compress_msg";
    public static final String MSG_FROM_SOURCE = "msg_from_source";
    public static final String ORIGIN_OFFSET = "origin_offset";

    public static final String ORIGIN_QUEUE_ID = "origin_queue_id";

    public static final String ORIGIN_QUEUE_IS_LONG = "origin_offset_is_LONG";

    public static final String ORIGIN_MESSAGE_HEADER = "origin_message_header";

    public static final String ORIGIN_SOURCE_NAME = "origin_offset_name";

    public static final String SHUFFLE_KEY = "SHUFFLE_KEY";

    public static final String ORIGIN_MESSAGE_TRACE_ID = "origin_request_id";
    protected transient boolean isWindowTest = false;
    protected transient AtomicLong COUNT = new AtomicLong(0);
    /**
     * 分片转发channel
     */
    protected transient ShuffleChannel shuffleChannel;

    public void initMiniBatch() {
        shuffleMsgCache  = new MiniBatchMsgCache(new WindowCache.MutilMsgMergerAndCompressFlushCallBack(),(IShuffleKeyGenerator) shuffleChannel.getWindow(),shuffleChannel.getWindow());


        shuffleMsgCache.openAutoFlush();
    }

//    protected transient AtomicLong insertCount=new AtomicLong(0);
//    protected transient AtomicLong shuffleCount=new AtomicLong(0);
//    protected transient AtomicLong SUM=new AtomicLong(0);

    protected class MutilMsgMergerAndCompressFlushCallBack implements IMessageFlushCallBack<Pair<ISplit, IMessage>> {

        @Override
        public boolean flushMessage(List<Pair<ISplit, IMessage>> messages) {
            if (messages == null || messages.size() == 0) {
                return true;
            }
            long start=System.currentTimeMillis();
            ISplit split = messages.get(0).getLeft();
            JSONArray allMsgs =new JSONArray();
            long sum=0;
            for (int i = 0; i < messages.size(); i++) {
                Pair<ISplit, IMessage> pair = messages.get(i);
                IMessage message = pair.getRight();
                allMsgs.add(message.getMessageBody());
               // sum=SUM.addAndGet(message.getMessageBody().getLong("total"));
            }
            //System.out.println("before shuffle sum is "+sum);
            JSONObject jsonObject=shuffleChannel.createMsg(allMsgs,split);
//            JSONObject zipJsonObject = new JSONObject();
//            zipJsonObject.put(COMPRESSION_MSG_DATA, CompressUtil.gZip(jsonObject.toJSONString()));
            shuffleChannel.getProducer().batchAdd(new Message(jsonObject), split);
            shuffleChannel.getProducer().flush(split.getQueueId());
            long cost=System.currentTimeMillis()-start;
           // shuffleCount.addAndGet(cost);
            return true;
        }
    }


    protected transient MiniBatchMsgCache shuffleMsgCache ;

    @Override
    protected boolean initConfigurable() {

        isWindowTest = ComponentCreator.getPropertyBooleanValue("window.fire.isTest");
        return super.initConfigurable();
    }

    @Override
    protected boolean batchInsert(List<IMessage> messageList) {
        long start=System.currentTimeMillis();
        for (IMessage msg : messageList) {
            String shuffleKey = generateShuffleKey(msg);
            IMessage message= ShuffleUtil.createShuffleMsg(msg,shuffleKey);
            if(message==null){
                continue;
            }
            addPropertyToMessage(msg, message.getMessageBody());
            Integer index = shuffleChannel.hash(shuffleKey);
            ISplit split = shuffleChannel.getSplit(index);
            shuffleMsgCache.addCache(new MutablePair(split, message));
        }
        if (isWindowTest) {
            long count = COUNT.addAndGet(messageList.size());
            System.out.println(shuffleChannel.getWindow().getConfigureName() + " send shuffle msg count is " + count);
            shuffleMsgCache.flush();
        }
        long cost=System.currentTimeMillis()-start;
        //shuffleCount.addAndGet(cost);
        return true;
    }

    @Override
    public void finishBatchMsg(BatchFinishMessage batchFinishMessage) {
        long start=System.currentTimeMillis();
        if (shuffleChannel != null && shuffleChannel.getProducer() != null) {
            this.flush();
            shuffleMsgCache.flush();
            for (ISplit split : shuffleChannel.getQueueList()) {
                IMessage message = batchFinishMessage.getMsg().deepCopy();
                message.getMessageBody().put(ORIGIN_QUEUE_ID, message.getHeader().getQueueId());
                shuffleChannel.getProducer().batchAdd(message, split);
            }
            shuffleChannel.getProducer().flush();
        }
       // System.out.println("insert cost is "+insertCount.get()+" shuffle cost is "+shuffleCount.get()+"  finish batch cost is "+(System.currentTimeMillis()-start));

    }



    /**
     * 根据message生成shuffle key
     *
     * @param message
     * @return
     */
    protected abstract String generateShuffleKey(IMessage message);

    @Override
    public boolean checkpoint(Set<String> queueIds) {
        this.flush(queueIds);
        this.shuffleMsgCache.flush(queueIds);
        return true;
    }

    /**
     * 如果需要额外的字段附加到shuffle前的message，通过实现这个子类增加
     *
     * @param oriJson
     */
    protected void addPropertyToMessage(IMessage oriMessage, JSONObject oriJson) {

    }

    public ShuffleChannel getShuffleChannel() {
        return shuffleChannel;
    }

    public MiniBatchMsgCache getShuffleMsgCache() {
        return this.shuffleMsgCache;
    }

    public void setShuffleChannel(ShuffleChannel shuffleChannel) {
        this.shuffleChannel = shuffleChannel;
    }
}
