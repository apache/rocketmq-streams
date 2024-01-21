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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.channel.builder.IChannelBuilder;
import org.apache.rocketmq.streams.common.channel.builder.IShuffleChannelBuilder;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemoryCache;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySink;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

/**
 * 创建shuffle source and sink
 */
public class ShuffleManager {

    protected static Map<AbstractWindow, Pair<ISource<?>, AbstractSupportShuffleSink>> cache = new HashMap<>();

    /**
     * 创建shuffle source and sink，先检查配置文件，如果没有配置，则自动创建
     *
     * @param pipelineSource 自动创建时，参考分片数量
     * @param window         需要shuffle的窗口
     * @return
     */
    public static synchronized Pair<ISource<?>, AbstractSupportShuffleSink> createOrGetShuffleSourceAndSink(ISource<?> pipelineSource, AbstractWindow window) {
        if (cache.get(window) != null) {
            return cache.get(window);
        }
        String sourceType = getSourceTypeFromConfigue(window);
        if (StringUtil.isNotEmpty(sourceType)) {
            Pair<ISource<?>, AbstractSupportShuffleSink> sourceISinkPair = createShuffleChannelByConfigure(window, sourceType);
            if (sourceISinkPair != null && sourceISinkPair.getLeft() != null && sourceISinkPair.getRight() != null) {
                cache.put(window, sourceISinkPair);
                return sourceISinkPair;
            }
        }
        Pair<ISource<?>, AbstractSupportShuffleSink> sourceISinkPair = autoCreateShuffleChannel(pipelineSource, window);
        if(sourceISinkPair.getLeft() instanceof AbstractSource){
            ((AbstractSource) sourceISinkPair.getLeft()).setJsonData(true);
            ((AbstractSource) sourceISinkPair.getLeft()).setMsgIsJsonArray(false);
        }
        cache.put(window, sourceISinkPair);
        return sourceISinkPair;
    }

    protected static String getSourceTypeFromConfigue(AbstractWindow window) {
        String type = ShuffleContext.getInstance().get();
        if (StringUtil.isNotEmpty(type)) {
            return type;
        }
        return window.getConfiguration().getProperty(ConfigurationKey.WINDOW_SHUFFLE_CHANNEL_TYPE);
    }

    public static Pair<ISource<?>, AbstractSupportShuffleSink> createShuffleChannelByConfigure(AbstractWindow window, String sourceType) {

        ISource<?> source = createSource(window, sourceType);
        AbstractSupportShuffleSink shuffleSink = createSink(window, sourceType, source);
        return Pair.of(source, shuffleSink);
    }

    /**
     * 如果用户未配置shuffle channel，根据pipeline数据源动态创建
     */
    public static Pair<ISource<?>, AbstractSupportShuffleSink> autoCreateShuffleChannel(ISource<?> pipelineSource, AbstractWindow window) {
        synchronized (window) {
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
            if (sink == null) {
                return null;
            }
            sink.init();
            if (!(sink instanceof MemorySink) && !(sink instanceof AbstractSupportShuffleSink)) {
                throw new RuntimeException("can not create shuffle channel, sink not extends AbstractSupportShuffleSink " + pipelineSource.toJson());
            }
            AbstractSupportShuffleSink abstractSupportShuffleSink = (AbstractSupportShuffleSink) sink;
            abstractSupportShuffleSink.setSplitNum(10);
            ISource<?> source = null;
            if (sink instanceof MemorySink) {
                MemoryCache memoryCache = new MemoryCache();
                memoryCache.setNameSpace(pipelineSource.getNameSpace());
                memoryCache.setName(createNameByWindow(window));

                sink = new MemorySink();
                source = new MemorySource();
                ((MemorySink) sink).setMemoryCache(memoryCache);
                ((MemorySource) source).setMemoryCache(memoryCache);
                memoryCache.init();
            }

            Properties properties = new Properties();
            properties.put("groupName", createNameByWindow(window));
            properties.put("tags", createNameByWindow(window));

            AbstractSupportShuffleSink shuffleSink = (AbstractSupportShuffleSink) sink;
            //todo 为什么这里的分区数量要和源头topic的分区数量一直？
            shuffleSink.setSplitNum(getShuffleSplitCount(shuffleSink));
            shuffleSink.setNameSpace(pipelineSource.getNameSpace());
            shuffleSink.setName(createNameByWindow(window));
            String topicFiledName = shuffleSink.getShuffleTopicFieldName();
            String shuffleTopic = null;
            //内存模式，是无topic的
            if (StringUtil.isNotEmpty(topicFiledName)) {
                String topic = ReflectUtil.getDeclaredField(shuffleSink, topicFiledName);
                shuffleTopic = createShuffleTopic(topic, pipelineSource);
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
            source.setName(sink.getName());
            //修改主题
            if (shuffleTopic != null && topicFiledName != null) {
                ReflectUtil.setBeanFieldValue(source, topicFiledName, shuffleTopic);
            }
            if (source instanceof AbstractSource) {
                AbstractSource abstractSource = (AbstractSource) source;
                abstractSource.setHasInit(false);
            }
            source.init();
            return Pair.of(source, shuffleSink);

        }
    }

    protected static ISource<?> createSource(AbstractWindow window, String sourceType) {
        IChannelBuilder builder = createChannelBuilder(sourceType);
        if (builder == null) {
            return null;
        }
        Properties properties = createChannelProperties(window);
        ISource<?> source = builder.createSource(window.getNameSpace(), window.getName(), properties, null);
        if (source instanceof MemorySource) {
            MemorySource memorySource = (MemorySource) source;
            MemoryCache memoryCache = new MemoryCache();
            memorySource.setMemoryCache(memoryCache);
            memorySource.setNameSpace(window.getNameSpace());
            memorySource.setName(window.getName());
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
    protected static AbstractSupportShuffleSink createSink(AbstractWindow window, String sourceType, ISource source) {

        IChannelBuilder builder = createChannelBuilder(sourceType);
        if (builder == null) {
            return null;
        }
        Properties properties = createChannelProperties(window);

        ISink<?> sink = builder.createSink(window.getNameSpace(), window.getName(), properties, null);
        if (!(sink instanceof AbstractSupportShuffleSink)) {
            throw new RuntimeException("can not support shuffle " + sink.toJson());
        }
        if (sink instanceof MemorySink) {
            MemorySink memorySink = (MemorySink) sink;
            if (!(source instanceof MemorySource)) {
                throw new RuntimeException("shuffle cosumer need memory, real is " + source);
            }
            MemorySource memorySource = (MemorySource) source;
            MemoryCache memoryCache = memorySource.getMemoryCache();
            memorySink.setMemoryCache(memoryCache);
            memorySink.setNameSpace(window.getNameSpace());
            memorySink.setName(window.getName());
        }

        sink.init();
        return (AbstractSupportShuffleSink) sink;
    }

    /**
     * 自动创建shuffle source的分片数，等于shuffle sink的，shufflesink=pipeline source的分片
     *
     * @param shuffleSink
     * @return
     */
    protected static int getShuffleSplitCount(AbstractSupportShuffleSink shuffleSink) {
        int splitNum = shuffleSink.getSplitNum();
        return splitNum > 0 ? splitNum : 32;
    }

    /**
     * 根据window 信息生成名字，用于source，sink，group等名字
     *
     * @param window
     * @return
     */
    protected static String createNameByWindow(AbstractWindow window) {
        String dynamicPropertyValue = MapKeyUtil.createKey(window.getNameSpace(), window.getName(), String.valueOf(window.getUpdateFlag()));
        dynamicPropertyValue = dynamicPropertyValue.replaceAll("\\.", "_").replaceAll(";", "_");
        return dynamicPropertyValue;
    }

    /**
     * 创建topic
     *
     * @param topic
     * @param pipelineSource
     * @return
     */
    protected static String createShuffleTopic(String topic, ISource pipelineSource) {
        return "shuffle_" + topic + "_" + pipelineSource.getNameSpace().replaceAll("\\.", "_") + "_" + pipelineSource.getName().replaceAll("\\.", "_").replaceAll(";", "_");
    }

    /**
     * 加载source和sink的builder
     *
     * @param type
     * @return
     */
    protected static IChannelBuilder createChannelBuilder(String type) {
        ServiceLoaderComponent serviceLoaderComponent = ComponentCreator.getComponent(IChannelBuilder.class.getName(), ServiceLoaderComponent.class);
        return (IChannelBuilder) serviceLoaderComponent.loadService(type);
    }

    /**
     * 根据属性文件配置
     *
     * @return 资源文件
     */
    protected static Properties createChannelProperties(AbstractWindow window) {
        Properties properties = new Properties();
        for (Map.Entry<Object, Object> entry : window.getConfiguration().entrySet()) {
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            if (key.startsWith(ConfigurationKey.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX)) {
                String channelKey = key.replace(ConfigurationKey.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX, "");
                if (channelKey.startsWith(window.getNameSpace())) {//支持基于namespace 做shuffle window共享
                    channelKey = channelKey.replace(window.getNameSpace(), "");
                    properties.put(channelKey, value);
                } else {
                    if (!properties.containsKey(channelKey)) {
                        properties.put(channelKey, value);
                    }
                }
            }
        }
        //TODO 其实只有rocketmq作为shuffle队列的时候，才需要设置这个参数， 这里后续需要修改
        properties.put("groupName", createNameByWindow(window));
        properties.put("tags", createNameByWindow(window));
        return properties;
    }

}
