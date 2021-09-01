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
package org.apache.rocketmq.streams.common.channel.builder;

import com.alibaba.fastjson.JSONObject;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 创建channel，如果需要扩展channel，需要实现这个接口，跟定属性文件，能够创建channel对象 如果想和sql对接，实现这个接口，properties中的kv是sql中with部分的内容
 */
public interface IChannelBuilder {

    /**
     * 主要完成sql中的with 属性和source/sink字段名的映射
     *
     * @param formatProperties
     * @param inputProperties
     * @param formatName
     * @param inputName
     */
    static void formatPropertiesName(JSONObject formatProperties, Properties inputProperties, String formatName,
        String inputName) {
        String inputValue = inputProperties.getProperty(inputName);
        if (StringUtil.isNotEmpty(inputValue)) {
            formatProperties.put(formatName, inputValue);
        }
    }

    /**
     * 创建channel
     *
     * @param properties
     * @return
     */
    ISource createSource(String namespace, String name, Properties properties, MetaData metaData);

    /**
     * 返回channel类型，和blink语句中的type值一致
     *
     * @return
     */
    String getType();

    /**
     * 创建channel
     *
     * @param properties
     * @return
     */
    ISink createSink(String namespace, String name, Properties properties, MetaData metaData);

}
