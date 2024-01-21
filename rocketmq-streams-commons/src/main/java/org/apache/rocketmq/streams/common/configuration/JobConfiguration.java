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
package org.apache.rocketmq.streams.common.configuration;

import java.util.Iterator;
import java.util.Properties;

public class JobConfiguration {
    private final Properties properties;

    public JobConfiguration() {
        this.properties = new Properties();
    }

    public JobConfiguration(Properties properties) {
        this.properties = properties;
    }

    public Properties getProperties() {
        return properties;
    }

    /**
     * 打开pipeline html monitor
     */
    public JobConfiguration openHtmlMonitor() {
        this.properties.put(ConfigurationKey.MONITOR_PIPELINE_HTML_SWITCH, "true");
        return this;
    }

    /**
     * 打开pipeline html monitor
     */
    public JobConfiguration stageDelayTime(int stageSlowSecond) {
        this.properties.put(ConfigurationKey.MONITOR_SLOW_TIMEOUT, stageSlowSecond);
        return this;

    }

    /**
     * 打开pipeline html monitor
     */
    public JobConfiguration windowSize(int windowSizeSecond) {
        this.properties.put(ConfigurationKey.DIPPER_WINDOW_DEFAULT_INTERVAL_SIZE, windowSizeSecond);
        return this;

    }

    /**
     * 打开pipeline html monitor
     */
    public JobConfiguration joinWindowSize(int windowSizeSecond, int windowCount) {
        this.properties.put(ConfigurationKey.DIPPER_WINDOW_JOIN_DEFAULT_INTERVAL_SIZE, windowSizeSecond);
        this.properties.put(ConfigurationKey.DIPPER_WINDOW_JOIN_RETAIN_WINDOW_COUNT, windowCount);
        return this;

    }

    /**
     * 打开pipeline html monitor
     */
    public JobConfiguration overWindowSize(int windowSizeSecond, int emitSecond) {
        this.properties.put(ConfigurationKey.DIPPER_WINDOW_OVER_DEFAULT_INTERVAL_SIZE, windowSizeSecond);
        this.properties.put(ConfigurationKey.DIPPER_WINDOW_OVER_DEFAULT_EMIT_BEFORE_SECOND, emitSecond);
        return this;

    }

    /**
     * 如果需要自定义shuffle，可以通过这个配置项实现
     *
     * @param channelType 和sql with部分的type相同
     * @param properties  和sql with部分的key/value相同
     * @return
     */
    public JobConfiguration shuffle(String channelType, Properties properties) {
        this.properties.put(ConfigurationKey.WINDOW_SHUFFLE_CHANNEL_TYPE, channelType);
        if (properties != null) {
            Iterator<?> it = properties.keySet().iterator();
            while (it.hasNext()){
                String key = it.next().toString();
                String prefixKey = ConfigurationKey.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX + key;
                this.properties.put(prefixKey, properties.get(key));
            }

        }
        return this;
    }

    public JobConfiguration config(String key, Object value) {
        this.properties.put(key, value);
        return this;
    }

}
