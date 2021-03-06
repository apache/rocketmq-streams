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
package org.apache.rocketmq.streams.window.shuffle;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.builder.IShuffleChannelBuilder;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemoryCache;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemoryChannel;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySink;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessageProcessor;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public abstract class AbstractSystemChannel implements IConfigurableIdentification, ISystemMessageProcessor, IStreamOperator {

    protected static final Log LOG = LogFactory.getLog(AbstractSystemChannel.class);

    protected static final String CHANNEL_PROPERTY_KEY_PREFIX = "CHANNEL_PROPERTY_KEY_PREFIX";
    protected static final String CHANNEL_TYPE = "CHANNEL_TYPE";

    protected ISource<?> consumer;
    protected AbstractSupportShuffleSink producer;
    protected Map<String, String> channelConfig = new HashMap<>();
    protected boolean hasCreateShuffleChannel = false;

    protected transient AtomicBoolean hasStart = new AtomicBoolean(false);
    public void startChannel() {
        if (consumer == null) {
            return;
        }
        if (hasStart.compareAndSet(false, true)) {
            consumer.start(this);
        }

    }


    /**
     * init shuffle channel
     */
    public void init(AbstractWindow window) {
        this.consumer = createSource(window.getNameSpace(), window.getConfigureName());
        this.producer = createSink(window.getNameSpace(), window.getConfigureName());
        if (this.consumer == null || this.producer == null) {
            autoCreateShuffleChannel(window.getFireReceiver().getPipeline());
        }
        if (this.consumer == null) {
            return;
        }
        if (this.consumer instanceof AbstractSource) {
            ((AbstractSource) this.consumer).setJsonData(true);
        }
        if(producer!=null){
            this.producer.init();
        }

    }
    /**
     * ?????????????????????shuffle channel?????????pipeline?????????????????????
     *
     * @param pipeline
     */
    public void autoCreateShuffleChannel(ChainPipeline pipeline) {
        if (!hasCreateShuffleChannel) {
            synchronized (this) {
                if (!hasCreateShuffleChannel) {
                    ISource<?> pipelineSource = pipeline.getSource();
                    ServiceLoaderComponent serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);

                    IChannelBuilder builder = (IChannelBuilder) serviceLoaderComponent.loadService(pipelineSource.getClass().getSimpleName());
                    if (builder == null) {
                        throw new RuntimeException("can not create shuffle channel, not find channel builder " + pipelineSource.toJson());
                    }
                    if (!(builder instanceof IShuffleChannelBuilder)) {
                        throw new RuntimeException("can not create shuffle channel, builder not impl IShuffleChannelBuilder " + pipelineSource.toJson());
                    }
                    IShuffleChannelBuilder shuffleChannelBuilder = (IShuffleChannelBuilder) builder;
                    ISink<?> sink = shuffleChannelBuilder.createBySource(pipelineSource);
                    sink.init();
                    if (!(sink instanceof MemoryChannel) && !(sink instanceof AbstractSupportShuffleSink)) {
                        throw new RuntimeException("can not create shuffle channel, sink not extends AbstractSupportShuffleSink " + pipelineSource.toJson());
                    }
                    ISource<?> source = null;
                    if (sink instanceof MemoryChannel) {
                        MemoryCache memoryCache = new MemoryCache();
                        memoryCache.setNameSpace(createShuffleChannelNameSpace(pipeline));
                        memoryCache.setConfigureName(createShuffleChannelName(pipeline));

                        sink = new MemorySink();
                        source = new MemorySource();
                        ((MemorySink) sink).setMemoryCache(memoryCache);
                        ((MemorySource) source).setMemoryCache(memoryCache);
                        memoryCache.init();
                    }

                    Properties properties = new Properties();
                    putDynamicPropertyValue(new HashSet<>(), properties);

                    AbstractSupportShuffleSink shuffleSink = (AbstractSupportShuffleSink) sink;
                    //todo ??????????????????????????????????????????topic????????????????????????
                    shuffleSink.setSplitNum(getShuffleSplitCount(shuffleSink));
                    shuffleSink.setNameSpace(createShuffleChannelNameSpace(pipeline));
                    shuffleSink.setConfigureName(createShuffleChannelName(pipeline));
                    String topicFiledName = shuffleSink.getShuffleTopicFieldName();
                    String shuffleTopic = null;
                    //?????????????????????topic???
                    if (StringUtil.isNotEmpty(topicFiledName)) {
                        String topic = ReflectUtil.getDeclaredField(shuffleSink, topicFiledName);
                        shuffleTopic = createShuffleTopic(topic, pipeline);
                        ReflectUtil.setBeanFieldValue(shuffleSink, topicFiledName, shuffleTopic);
                    }

                    //?????????window?????????????????????groupname???tags
                    List<Field> fields = ReflectUtil.getDeclaredFieldsContainsParentClass(sink.getClass());
                    for (Field field : fields) {
                        String fieldName = field.getName();
                        String value = properties.getProperty(fieldName);
                        if (StringUtil.isNotEmpty(value)) {
                            ReflectUtil.setBeanFieldValue(sink, fieldName, value);
                        }
                    }
                    shuffleSink.setHasInit(false);
                    shuffleSink.init();//???????????????shuffle channel?????????
                    if (source == null) {
                        source = shuffleChannelBuilder.copy(pipelineSource);
                    }

                    //?????????window?????????????????????groupname???tags

                    fields = ReflectUtil.getDeclaredFieldsContainsParentClass(source.getClass());
                    for (Field field : fields) {
                        String fieldName = field.getName();
                        String value = properties.getProperty(fieldName);
                        if (StringUtil.isNotEmpty(value)) {
                            ReflectUtil.setBeanFieldValue(source, fieldName, value);
                        }
                    }

                    source.setNameSpace(sink.getNameSpace());
                    source.setConfigureName(sink.getConfigureName());
                    //????????????
                    if (shuffleTopic != null && topicFiledName != null) {
                        ReflectUtil.setBeanFieldValue(source, topicFiledName, shuffleTopic);
                    }
                    if (source instanceof AbstractSource) {
                        AbstractSource abstractSource = (AbstractSource) source;
                        abstractSource.setHasInit(false);
                    }
                    source.init();

                    this.producer = shuffleSink;
                    this.consumer = source;
                }
            }
        }
    }

    /**
     * ??????????????????????????????shufflechannel????????????
     *
     * @param topic
     * @param message
     * @return
     */
    protected abstract String createShuffleTopic(String topic, ChainPipeline message);

    protected abstract int getShuffleSplitCount(AbstractSupportShuffleSink shuffleSink);

    /**
     * shuffle channel?????????
     *
     * @param message
     * @return
     */
    protected abstract String createShuffleChannelName(ChainPipeline message);

    /**
     * shuffle channel?????????
     *
     * @param message
     * @return
     */
    protected abstract String createShuffleChannelNameSpace(ChainPipeline message);

    protected Map<String, String> getChannelConfig() {
        return channelConfig;
    }

    protected abstract String getDynamicPropertyValue();

    /**
     * ??????channel???????????????????????????channel???????????????
     *
     * @return
     */
    protected ISource<?> createSource(String namespace, String name) {
        IChannelBuilder builder = createBuilder();
        if (builder == null) {
            return null;
        }
        Properties properties = createChannelProperties(namespace);
        ISource<?> source = builder.createSource(namespace, name, properties, null);
        if (source instanceof MemorySource) {
            MemorySource memorySource = (MemorySource) source;
            MemoryCache memoryCache = new MemoryCache();
            memorySource.setMemoryCache(memoryCache);
            memoryCache.init();
        }
        source.init();
        return source;
    }

    /**
     * ??????channel???????????????????????????channel???????????????
     *
     * @return
     */
    protected AbstractSupportShuffleSink createSink(String namespace, String name) {

        IChannelBuilder builder = createBuilder();
        if (builder == null) {
            return null;
        }
        Properties properties = createChannelProperties(namespace);

        ISink<?> sink = builder.createSink(namespace, name, properties, null);
        if (!(sink instanceof AbstractSupportShuffleSink)) {
            throw new RuntimeException("can not support shuffle " + sink.toJson());
        }
        if (sink instanceof MemorySink) {
            MemorySink memorySink = (MemorySink) sink;
            if (!(this.consumer instanceof MemorySource)) {
                throw new RuntimeException("shuffle cosumer need memory, real is " + this.consumer);
            }
            MemorySource memorySource = (MemorySource) this.consumer;
            MemoryCache memoryCache = memorySource.getMemoryCache();
            memorySink.setMemoryCache(memoryCache);
        }

        sink.init();
        return (AbstractSupportShuffleSink) sink;
    }

    /**
     * create channel builder
     *
     * @return
     */
    protected IChannelBuilder createBuilder() {
        String type = ComponentCreator.getProperties().getProperty(getChannelConfig().get(CHANNEL_TYPE));
        if (StringUtil.isEmpty(type)) {
            return null;
        }
        ServiceLoaderComponent serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        return (IChannelBuilder) serviceLoaderComponent.loadService(type);
    }

    /**
     * ????????????????????????
     *
     * @return ????????????
     */
    protected Properties createChannelProperties(String namespace) {
        Properties properties = new Properties();
        for (Map.Entry<Object, Object> entry : ComponentCreator.getProperties().entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.startsWith(getChannelConfig().get(CHANNEL_PROPERTY_KEY_PREFIX))) {
                String channelKey = key.replace(getChannelConfig().get(CHANNEL_PROPERTY_KEY_PREFIX), "");
                if (channelKey.startsWith(namespace)) {//????????????namespace ???shuffle window??????
                    channelKey = channelKey.replace(namespace, "");
                    properties.put(channelKey, value);
                } else {
                    if (!properties.containsKey(channelKey)) {
                        properties.put(channelKey, value);
                    }
                }
            }
        }
        Set<String> multiPropertySet = new HashSet<>();
        String dynamicProperty = properties.getProperty("dynamic.property");
        if (dynamicProperty != null) {

            String dynamicPropertyValue = getDynamicPropertyValue();
            String[] mutilPropertys = dynamicProperty.split(",");

            for (String properyKey : mutilPropertys) {
                properties.put(properyKey, dynamicPropertyValue);
                multiPropertySet.add(properyKey);
            }

        }
        putDynamicPropertyValue(multiPropertySet, properties);
        return properties;
    }

    /**
     * ?????????????????????????????????????????????????????????
     *
     * @param dynamicPropertySet ?????????
     */
    protected void putDynamicPropertyValue(Set<String> dynamicPropertySet, Properties properties) {

    }

    public ISource<?> getConsumer() {
        return consumer;
    }

    public ISink<?> getProducer() {
        return producer;
    }

    public void sendMessage(IMessage message) {
        List<IMessage> msgs = new ArrayList<>();
        msgs.add(message);
        producer.batchSave(msgs);
        producer.flush();
    }

}
