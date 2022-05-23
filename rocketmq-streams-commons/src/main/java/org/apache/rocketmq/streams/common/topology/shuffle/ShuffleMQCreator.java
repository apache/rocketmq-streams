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
package org.apache.rocketmq.streams.common.topology.shuffle;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
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
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.ServiceLoadUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * create shuffle source and producer
 */
public class ShuffleMQCreator {
    private static ICache<String,ShuffleMQCreator> cache=new SoftReferenceCache<>();
    public static String SHUFFLE_BUILTIN="built-in";
    /**
     * input parameters
     */
    protected String namespace;
    protected String pipelineName;
    protected String shuffleOwnerName;
    protected int splitCount;
    protected transient ISource pipelineSource;

    protected transient List<ISplit<?, ?>> queueList;//shuffle mq's split list
    protected transient Map<String, ISplit<?, ?>> queueMap = new ConcurrentHashMap<>();


    /**
     * need create
     */
    protected transient AbstractSupportShuffleSink producer;
    protected ISource consumer;

    /**
     *
     */
    protected volatile boolean hasCreateShuffleChannel = false;

    private ShuffleMQCreator(ISource source,String namespace,String pipelineName,String shuffleOwnerName, int splitCount){
        this.pipelineSource=source;
        this.namespace=namespace;
        this.pipelineName=pipelineName;
        this.shuffleOwnerName=shuffleOwnerName;
        this.splitCount=splitCount;
    }

    public static ShuffleMQCreator createShuffleCreator(ISource source,String namespace,String pipelineName,String shuffleOwnerName, int splitCount){
        String key= MapKeyUtil.createKey(namespace,pipelineName,shuffleOwnerName);
        ShuffleMQCreator shuffleMQCreator=cache.get(key);
        if(shuffleMQCreator!=null){
            return shuffleMQCreator;
        }
        shuffleMQCreator=new ShuffleMQCreator(source,namespace,pipelineName,shuffleOwnerName,splitCount);
        shuffleMQCreator.init();
        cache.put(key,shuffleMQCreator);
        return shuffleMQCreator;
    }


    public static ISource getSource(String namespace,String pipelineName,String shuffleOwnerName ){
        String key= MapKeyUtil.createKey(namespace,pipelineName,shuffleOwnerName);
        ShuffleMQCreator shuffleMQCreator=cache.get(key);
        if(shuffleMQCreator==null){
            throw new RuntimeException("expect get  ShuffleMQCreator in cache, but not. Check whether it is created in the shuffleproducerchainstage class ");
        }
        return shuffleMQCreator.getConsumer();
    }





    /**
     * init shuffle channel
     */
    private void init() {
        String channelType =this.pipelineSource.getClass().getSimpleName();
        if(SHUFFLE_BUILTIN.equals(channelType)){
            ISource source=createSourceByProperty(namespace, pipelineName);
            source.setNameSpace(namespace);
            autoCreateShuffleChannel(source);
        }else{
            consumer= createSourceByProperty(namespace, pipelineName);
            producer = createSinkByProperty(namespace, pipelineName);
            if (consumer == null || producer== null) {
                autoCreateShuffleChannel(pipelineSource);
            }
            if (consumer == null) {
                return;
            }
            if (consumer instanceof AbstractSource) {
                ((AbstractSource) consumer).setJsonData(true);
            }
            if(producer!=null){
                producer.init();
            }
            if (producer != null && (queueList == null || queueList.size() == 0)) {
                this.producer.init();
                queueList = producer.getSplitList();
                Map<String, ISplit<?, ?>> tmp = new ConcurrentHashMap<>();
                for (ISplit<?, ?> queue : queueList) {
                    tmp.put(queue.getQueueId(), queue);
                }
                this.queueMap = tmp;
            }
        }


    }
    /**
     * choose shuffle split
     * @param key
     * @return
     */
    public int hash(Object key) {
        int mValue = queueList.size();
        int h = 0;
        if (key != null) {
            h = key.hashCode() ^ (h >>> 16);
            if (h < 0) {
                h = -h;
            }
        }
        return h % mValue;
    }


    /**
     * 创建channel，根据配置文件配置channel的连接信息
     *
     * @return
     */
    protected ISource<?> createSourceByProperty(String namespace, String name) {
        IChannelBuilder builder = createBuilder();
        if (builder == null) {
            return null;
        }
        Properties properties = createChannelProperties(namespace,this.shuffleOwnerName);
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
     * 创建channel，根据配置文件配置channel的连接信息
     *
     * @return
     */
    protected AbstractSupportShuffleSink createSinkByProperty(String namespace, String name) {

        IChannelBuilder builder = createBuilder();
        if (builder == null) {
            return null;
        }
        Properties properties = createChannelProperties(namespace,this.shuffleOwnerName);

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
     * 如果用户未配置shuffle channel，根据pipeline数据源动态创建
     *
     */
    public void autoCreateShuffleChannel(ISource pipelineSource) {
        if (!hasCreateShuffleChannel) {
            synchronized (this) {
                if (!hasCreateShuffleChannel) {
                    String channelType =this.pipelineSource.getClass().getSimpleName();
                    IChannelBuilder builder = ServiceLoadUtil.loadService(IChannelBuilder.class,channelType);
                    if (builder == null) {
                        throw new RuntimeException("can not create shuffle channel, not find channel builder by the type" + channelType);
                    }
                    if (!(builder instanceof IShuffleChannelBuilder)) {
                        throw new RuntimeException("can not create shuffle channel, builder not impl IShuffleChannelBuilder " + channelType);
                    }
                    IShuffleChannelBuilder shuffleChannelBuilder = (IShuffleChannelBuilder) builder;
                    ISink<?> sink = shuffleChannelBuilder.createBySource(pipelineSource);
                    sink.init();
                    if (!(sink instanceof MemoryChannel) && !(sink instanceof AbstractSupportShuffleSink)) {
                        throw new RuntimeException("can not create shuffle channel, sink not extends AbstractSupportShuffleSink " + channelType);
                    }
                    ISource<?> source = null;
                    if (sink instanceof MemoryChannel) {
                        MemoryCache memoryCache = new MemoryCache();
                        memoryCache.setNameSpace(namespace);
                        memoryCache.setConfigureName(shuffleOwnerName);

                        sink = new MemorySink();
                        source = new MemorySource();
                        ((MemorySink) sink).setMemoryCache(memoryCache);
                        ((MemorySource) source).setMemoryCache(memoryCache);
                        memoryCache.init();
                    }

                    Properties properties = new Properties();

                    properties.put("groupName", shuffleOwnerName);
                    properties.put("tags", shuffleOwnerName);

                    AbstractSupportShuffleSink shuffleSink = (AbstractSupportShuffleSink) sink;
                    shuffleSink.setSplitNum(getShuffleSplitCount(shuffleSink));
                    shuffleSink.setNameSpace(namespace);
                    shuffleSink.setConfigureName(shuffleOwnerName);
                    String topicFiledName = shuffleSink.getShuffleTopicFieldName();
                    String shuffleTopic = null;
                    //内存模式，是无topic的
                    if (StringUtil.isNotEmpty(topicFiledName)) {
                        String topic = ReflectUtil.getDeclaredField(shuffleSink, topicFiledName);
                        shuffleTopic = createShuffleTopic(topic);
                        ReflectUtil.setBeanFieldValue(shuffleSink, topicFiledName, shuffleTopic);
                    }

                    //修改和window有关的属性，如groupname，tags
                    List<Field> fields = ReflectUtil.getDeclaredFieldsContainsParentClass(sink.getClass());
                    for (Field field : fields) {
                        String fieldName = field.getName();
                        String value = properties.getProperty(fieldName);
                        if (StringUtil.isNotEmpty(value)) {
                            ReflectUtil.setBeanFieldValue(sink, fieldName, value);
                        }
                    }
                    shuffleSink.setHasInit(false);
                    shuffleSink.init();//在这里完成shuffle channel的创建
                    if (source == null) {
                        source = shuffleChannelBuilder.copy(pipelineSource);
                    }

                    //修改和window有关的属性，如groupname，tags

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
                    //修改主题
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
     * create channel builder
     *
     * @return
     */
    protected IChannelBuilder createBuilder() {
        String type = ComponentCreator.getProperties().getProperty(ConfigureFileKey.WINDOW_SHUFFLE_CHANNEL_TYPE);
        if (StringUtil.isEmpty(type)) {
            return null;
        }
        return ServiceLoadUtil.loadService(IChannelBuilder.class,type);
    }


    /**
     * 根据属性文件配置
     *
     * @return 资源文件
     */
    protected Properties createChannelProperties(String namespace,String shuffleOwnerName) {
        Properties properties = new Properties();
        for (Map.Entry<Object, Object> entry : ComponentCreator.getProperties().entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.startsWith(ConfigureFileKey.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX)) {
                String channelKey = key.replace(ConfigureFileKey.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX, "");
                if (channelKey.startsWith(namespace)) {//支持基于namespace 做shuffle window共享
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

            String dynamicPropertyValue = shuffleOwnerName;
            String[] mutilPropertys = dynamicProperty.split(",");

            for (String properyKey : mutilPropertys) {
                properties.put(properyKey, dynamicPropertyValue);
                multiPropertySet.add(properyKey);
            }

        }
        String groupName = "groupName";
        if (!multiPropertySet.contains(groupName)) {
            properties.put(groupName, shuffleOwnerName);
        }
        if (!multiPropertySet.contains("tags")) {
            properties.put("tags", shuffleOwnerName);
        }
        return properties;
    }

    protected int getShuffleSplitCount(AbstractSupportShuffleSink shuffleSink) {
        if(this.splitCount>0){
            return splitCount;
        }
        int splitNum = shuffleSink.getSplitNum();
        return splitNum > 0 ? splitNum : 32;
    }

    public List<ISplit<?, ?>> getQueueList() {
        return queueList;
    }

    /**
     * 1个pipeline一个 shuffle topic
     *
     * @param topic
     * @return
     */
    protected String createShuffleTopic(String topic) {
        return "shuffle_" + topic + "_" + namespace.replaceAll("\\.", "_") + "_" + pipelineName.replaceAll("\\.", "_").replaceAll(";", "_");
    }

    public AbstractSupportShuffleSink getProducer() {
        return producer;
    }

    public ISource getConsumer() {
        return consumer;
    }
}
