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
package org.apache.rocketmq.streams.dispatcher.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.consumer.DefaultMQPushConsumerImpl;
import org.apache.rocketmq.client.impl.consumer.RebalancePushImpl;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.RocketmqUtil;
import org.apache.rocketmq.streams.dispatcher.ICache;
import org.apache.rocketmq.streams.dispatcher.IDispatcher;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;
import org.apache.rocketmq.streams.dispatcher.IStrategy;
import org.apache.rocketmq.streams.dispatcher.cache.RocketmqCache;
import org.apache.rocketmq.streams.dispatcher.entity.DispatcherMapper;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.strategy.LeastStrategy;
import org.apache.rocketmq.streams.dispatcher.strategy.StrategyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RocketmqDispatcher<T> implements IDispatcher<T> {

    public static final String MAPPER_KEY = "mapper_key";
    public static final String NAMESPACE_CONFIG_SUFFIX = "_config";
    public static final String NAMESPACE_STATUS_SUFFIX = "_status";
    public static final String CLUSTER_NAME = "DefaultCluster";
    private static final Logger LOGGER = LoggerFactory.getLogger(RocketmqDispatcher.class);
    private final String instanceName;
    private final String dispatchGroup;
    private final String voteTopic;
    private final DispatchMode dispatchMode;
    private final IDispatcherCallback<?> dispatcherCallback;
    private final ICache cache;
    private final String nameServer;
    protected transient AtomicBoolean isStart = new AtomicBoolean(false);
    protected transient TreeSet<String> consumerIdList = Sets.newTreeSet();
    protected transient TreeSet<String> taskList = Sets.newTreeSet();
    protected transient Set<String> localCache = Sets.newHashSet();
    protected transient String className;
    protected transient String methodName;
    protected transient boolean isMaster = false;
    protected transient Long lastTimestamp = System.currentTimeMillis();
    private DefaultMQPushConsumer voteListener;
    private DefaultMQPushConsumer messageListener;
    private DefaultMQProducer messageSender;

    public RocketmqDispatcher(String nameServer, String voteTopic, String instanceName, String dispatchGroup, DispatchMode dispatchMode, IDispatcherCallback dispatcherCallback) {
        this(nameServer, voteTopic, instanceName, dispatchGroup, dispatchMode, dispatcherCallback, new RocketmqCache(nameServer));

    }

    public RocketmqDispatcher(String nameServer, String voteTopic, String instanceName, String dispatchGroup, DispatchMode dispatchMode, IDispatcherCallback dispatcherCallback, ICache cache) {
        this.nameServer = nameServer;
        this.voteTopic = voteTopic;
        this.dispatchGroup = dispatchGroup;
        this.instanceName = instanceName;
        this.dispatchMode = dispatchMode;
        this.dispatcherCallback = dispatcherCallback;
        this.cache = cache;

        //一个分片可以确保我的消息都是顺序的
        RocketmqUtil.createTopic(nameServer, this.voteTopic, 1, CLUSTER_NAME, false);
        RocketmqUtil.createTopic(nameServer, this.dispatchGroup, 1, CLUSTER_NAME, false);

        this.className = Thread.currentThread().getStackTrace()[2].getClassName();
        this.methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    @Override
    public void start() throws Exception {
        try {
            if (this.isStart.compareAndSet(false, true)) {
                if (this.messageSender == null) {
                    //用于向message topic 发送消息
                    this.messageSender = RocketmqUtil.initDefaultMQProducer(this.nameServer, this.instanceName, this.dispatchGroup);
                    LOGGER.info("[{}] MessageSender_Init_Success", this.instanceName);
                }
                if (this.messageListener == null) {
                    //监听message， 如果有消息过来, 判断过来的消息是启动还是停止， 根据消息的种类去调用不同的任务
                    this.messageListener = RocketmqUtil.initDefaultMQPushConsumer(this.nameServer, this.instanceName, this.dispatchGroup + "_" + this.instanceName, this.dispatchGroup);
                    this.messageListener.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                        try {
                            for (MessageExt ext : msgs) {
                                String[] messageTags = ext.getTags().split("_");

                                String tags = messageTags[0];
                                String requestId = messageTags[1];
                                long timestamp = messageTags.length > 2 ? Long.parseLong(messageTags[2]) : 0;
                                if (timestamp < lastTimestamp) {
                                    LOGGER.info("[{}] Dispatcher_Mapper_Newer_Than_Last_({}<{})", this.instanceName, timestamp, lastTimestamp);
                                    continue;
                                }
                                lastTimestamp = timestamp;

                                String receivedMessage = new String(ext.getBody(), StandardCharsets.UTF_8);
                                DispatcherMapper receivedDispatcherMapper = DispatcherMapper.parse(receivedMessage);
                                String currentMessage = this.cache.getKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY);
                                DispatcherMapper currentDispatcherMapper = DispatcherMapper.parse(currentMessage);

                                if (currentMessage != null && !receivedDispatcherMapper.equals(currentDispatcherMapper)) {
                                    //接收到的信息和中心存储的不一样，说明目前接受到的不是最新的，不做处理，继续等待
                                    continue;
                                }
                                LOGGER.info("[{}] Received_Message({})_Local({})_Tags({})", this.instanceName, receivedMessage, String.join(",", localCache), tags);
                                Set<String> taskSet = receivedDispatcherMapper.getTasks(this.instanceName);
                                if ("stop".equals(tags)) {
                                    //执行停止指令
                                    List<String> needStop = (List<String>) CollectionUtils.subtract(localCache, taskSet);
                                    if (!needStop.isEmpty()) {
                                        List<String> stopSuccess = this.dispatcherCallback.stop(Lists.newArrayList(needStop));
                                        if (stopSuccess != null && !stopSuccess.isEmpty()) {
                                            stopSuccess.forEach(localCache::remove);
                                        }
                                        LOGGER.info("[{}] Received_Stop_Message_NeedStop({})_Stopped({})_From({}.{})_({})", this.instanceName, String.join(",", needStop), stopSuccess == null ? "" : String.join(",", stopSuccess), className, methodName, requestId);
                                    }
                                    this.cache.putKeyConfig(this.dispatchGroup + NAMESPACE_STATUS_SUFFIX, this.instanceName, "stop");
                                } else if ("start".equals(tags)) {
                                    //执行启动
                                    List<String> needStart = (List<String>) CollectionUtils.subtract(taskSet, localCache);
                                    if (!needStart.isEmpty()) {
                                        //启动前，重新加载一下内存中的任务实例
                                        this.dispatcherCallback.list();
                                        List<String> startSuccess = this.dispatcherCallback.start(Lists.newArrayList(needStart));
                                        if (startSuccess != null && !startSuccess.isEmpty()) {
                                            localCache.addAll(startSuccess);
                                        }
                                        LOGGER.info("[{}] Received_Start_Message_NeedStart({})_Started({})_FROM({}.{})_({})", this.instanceName, String.join(",", needStart), startSuccess == null ? "" : String.join(",", startSuccess), className, methodName, requestId);
                                    }
                                    this.cache.putKeyConfig(this.dispatchGroup + NAMESPACE_STATUS_SUFFIX, this.instanceName, "start");
                                }
                            }
                        } catch (Exception e) {
                            LOGGER.error("[{}] Received_Message_Error", this.instanceName, e);
                        }
                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    });

                    LOGGER.info("[{}] MessageListener_Init_Success", this.instanceName);
                }

                if (this.voteListener == null) {
                    this.voteListener = RocketmqUtil.initDefaultMQPushConsumer(this.nameServer, this.instanceName, this.dispatchGroup, this.voteTopic);
                    DefaultMQPushConsumerImpl impl = ReflectUtil.getDeclaredField(this.voteListener, "defaultMQPushConsumerImpl");
                    ReflectUtil.setBeanFieldValue(impl, "rebalanceImpl", new RebalancePushImpl(impl) {
                        @Override
                        public boolean doRebalance(boolean isOrder) {
                            super.doRebalance(isOrder);
                            String requestId = UUID.randomUUID().toString();
                            try {
                                doDispatch(requestId, mQClientFactory, 0);
                                return true;
                            } catch (Exception e) {
                                LOGGER.error("[{}] Dispatcher_Execute_Error_({})", instanceName, requestId, e);
                                return false;
                            }
                        }
                    });
                    LOGGER.info("[{}] VoteListener_Init_Success", this.instanceName);
                }

                //重置offset， 从当前时间开始消费
                RocketmqUtil.resetOffsetNew(this.nameServer, this.dispatchGroup + "_" + this.instanceName, this.dispatchGroup, System.currentTimeMillis());
                this.messageSender.start();
                this.messageListener.start();
                this.voteListener.start();
            }
        } catch (Exception e) {
            this.isStart.set(false);
            throw new RuntimeException("Dispatcher_Start_Error", e);
        }
    }

    public synchronized void doDispatch(String requestId, MQClientInstance mqClientInstance, int depth) throws Exception {
        if (depth > 2) {
            return;
        }

        TreeSet<String> tmpConsumerIdList = Sets.newTreeSet();
        List<String> consumerList = mqClientInstance.findConsumerIdList(this.voteTopic, this.dispatchGroup);
        if (consumerList != null && !consumerList.isEmpty()) {
            for (String consumerId : consumerList) {
                tmpConsumerIdList.add(consumerId.substring(consumerId.indexOf("@") + 1));
            }
            boolean isConsumerChange = !consumerIdList.equals(tmpConsumerIdList);
            LOGGER.info("[{}] Cluster_CurrentInstanceList({})_NewInstanceList({})_IsChanged({})_({})", instanceName, String.join(",", consumerIdList), String.join(",", tmpConsumerIdList), isConsumerChange, requestId);
            if (isConsumerChange) {
                //当consumer发生变化时， 触发选主操作
                consumerIdList = tmpConsumerIdList;
                if (!tmpConsumerIdList.isEmpty()) {
                    String masterInstance = tmpConsumerIdList.iterator().next();
                    //选主成功
                    if (instanceName.equals(masterInstance)) {
                        isMaster = true;
                        LOGGER.info("[{}] Master", instanceName);
                    } else {
                        LOGGER.info("[{}] Slave", instanceName);
                    }
                }
            }
            if (isMaster) {
                if (!consumerIdList.isEmpty()) {
                    TreeSet<String> tasks = Sets.newTreeSet(this.dispatcherCallback.list());//此逻辑必须放在选主之前，因为所有实例都需要加载任务
                    boolean isTaskChange = !taskList.equals(tasks);
                    LOGGER.info("[{}] Task_CurrentTask({})_NewTask({})_IsChanged({})_({})", instanceName, String.join(", ", taskList), String.join(",", tasks), isTaskChange, requestId);
                    if (isTaskChange || isConsumerChange) {
                        //计算新的调度公式
                        IStrategy iStrategy = StrategyFactory.getStrategy(this.dispatchMode);
                        if (iStrategy instanceof LeastStrategy) {//如果是Least策略，则需要通过cache获取到当前的调度状态
                            String currentMessage = this.cache.getKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY);
                            if (currentMessage != null && !currentMessage.isEmpty()) {
                                DispatcherMapper currentDispatcherMapper = DispatcherMapper.parse(currentMessage);
                                ((LeastStrategy) iStrategy).setCurrentDispatcherMapper(currentDispatcherMapper);
                            }
                        }
                        DispatcherMapper dispatcherMapper = iStrategy.dispatch(Lists.newArrayList(tasks), Lists.newArrayList(consumerIdList));
                        String currentMessage = this.cache.getKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY);
                        DispatcherMapper currentDispatcherMapper = DispatcherMapper.parse(currentMessage);
                        if (currentMessage != null && dispatcherMapper.equals(currentDispatcherMapper)) {
                            LOGGER.info("[{}] Dispatcher_Result_Not_Changed_({})", this.instanceName, requestId);
                        } else {
                            LOGGER.info("[{}] Dispatcher_Result_Changed_CurrentResult({})_NewResult({})_({})", this.instanceName, currentMessage == null ? "{}" : currentMessage, dispatcherMapper, requestId);
                            //确认需要推送新的调度列表
                            this.cache.putKeyConfig(this.dispatchGroup + NAMESPACE_CONFIG_SUFFIX, MAPPER_KEY, dispatcherMapper.toString());
                        }

                        long currentTime = System.currentTimeMillis();
                        //推送stop指令
                        this.messageSender.send(new Message(this.dispatchGroup, "stop_" + requestId + "_" + currentTime, dispatcherMapper.toString().getBytes(StandardCharsets.UTF_8)), new SendCallback() {
                            @Override
                            public void onSuccess(SendResult sendResult) {
                                LOGGER.info("[{}] Send_Stop_Message_({}.{}.{}_{})", instanceName, className, methodName, depth, requestId);
                            }

                            @Override
                            public void onException(Throwable e) {
                                LOGGER.error("[{}] Send_Stop_Message_Error_({}.{}.{}_{})", instanceName, className, methodName, depth, requestId, e);
                            }
                        });

                        //当检查到所有节点都已经完成上一轮的同步， 推送start停止指令
                        boolean ifWait = pushWait(requestId, mqClientInstance, Lists.newArrayList(consumerIdList), depth);
                        if (ifWait) {
                            Message message = new Message(this.dispatchGroup, "start_" + requestId + "_" + currentTime, dispatcherMapper.toString().getBytes(StandardCharsets.UTF_8));
                            this.messageSender.send(message, new SendCallback() {
                                @Override
                                public void onSuccess(SendResult sendResult) {
                                    LOGGER.info("[{}] Send_Start_Message_({}.{}.{}_{})", instanceName, className, methodName, depth, requestId);
                                }

                                @Override
                                public void onException(Throwable e) {
                                    LOGGER.error("[{}] Send_Start_Message_Error_({}.{}.{}_{})", instanceName, className, methodName, depth, requestId, e);
                                }
                            });
                            //调度完成后重置taskList
                            taskList = tasks;
                        }
                    }
                }
            }
        } else {
            LOGGER.warn("[{}] ConsumerList_Empty", instanceName);
        }

    }

    private boolean pushWait(String requestId, MQClientInstance mQClientFactory, List<String> consumerIdList, int depth) throws Exception {
        boolean pushTag = false;
        //发送之前需要先查看各个客户端是否已经结束了上一轮的调度
        for (int i = 0; i < 3; i++) {
            boolean stateTag = true;
            for (String consumeId : consumerIdList) {
                String instanceId = consumeId.substring(consumeId.indexOf("@") + 1);
                String localStatus = this.cache.getKeyConfig(this.dispatchGroup + NAMESPACE_STATUS_SUFFIX, instanceId);
                if (localStatus == null || !localStatus.equals("stop")) {
                    stateTag = false;
                    LOGGER.info("[{}] Instance_Status_({}-{})", instanceName, instanceId, localStatus);
                    break;
                }
            }
            if (stateTag) {
                pushTag = true;
                break;
            } else {
                Thread.sleep(5000);
            }
        }
        if (!pushTag) {
            doDispatch(requestId, mQClientFactory, depth + 1);
        }
        return pushTag;
    }

    @Override
    public void wakeUp() throws Exception {
        MQClientInstance mqClientInstance = MQClientManager.getInstance().getOrCreateMQClientInstance(this.voteListener);
        mqClientInstance.rebalanceImmediately();
    }

    @Override
    public void close() {
        try {
            if (this.isStart.compareAndSet(true, false)) {
                if (this.voteListener != null) {
                    this.voteListener.shutdown();
                    this.voteListener = null;
                }
                if (this.messageSender != null) {
                    this.messageSender.shutdown();
                    this.messageSender = null;
                }
                if (this.messageListener != null) {
                    this.messageListener.shutdown();
                    this.messageListener = null;
                }
                if (this.consumerIdList != null) {
                    this.consumerIdList.clear();
                }
                if (this.localCache != null) {
                    this.localCache.clear();
                }
            }
        } catch (Exception e) {
            this.isStart.set(true);
            throw new RuntimeException("Dispatcher_Close_Error", e);
        }
    }

}
