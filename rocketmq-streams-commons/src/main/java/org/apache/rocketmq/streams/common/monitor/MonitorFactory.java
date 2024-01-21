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
package org.apache.rocketmq.streams.common.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.interfaces.ITaskStart;
import org.apache.rocketmq.streams.common.logger.LoggerOutputChannel;
import org.apache.rocketmq.streams.common.monitor.impl.DipperMonitor;

public class MonitorFactory {
    public static final String ALL_NAMESPACE_PIPLINES = "all.namespaces.piplines";//所有的pipline
    public static final String NAMESPACE_PIPLINES = "namespace.";//所有的pipline
    public static final String PIPLINE_START_UP = "startup";
    public static final String PIPLINE_START_UP_ERROR = "errorChannels";
    private static final ICache<String, IMonitor> monitorICache = new SoftReferenceCache();
    private static final List<ISink<?>> defaultOutput = new ArrayList<>();
    private static final Map<String, List<ISink<?>>> outputs = new HashMap<>();
    private static final Map<String, ISink<?>> loggerOutputChannelMap = new HashMap<>();
    public static String LOG_ROOT_DIR = "/tmp/dipper/logs";
    private static ISink<?> loggerOutputDataSource;//默认的输出channel，输出到日志

    public static IMonitor createMonitor(String name) {
        IMonitor monitor = new DipperMonitor();
        monitor.startMonitor(name);
        return monitor;
    }

    public static IMonitor getOrCreateMonitor(String name) {
        IMonitor monitor = monitorICache.get(name);
        if (monitor != null) {
            return monitor;
        }
        monitor = createMonitor(name);
        monitorICache.put(name, monitor);
        return monitor;
    }

    /**
     * 给指定的名字增加channel，此类输出会输出到这些channel
     *
     * @param name
     * @param channels
     */
    public static void addChannel(String name, ISink<?>... channels) {
        List<ISink<?>> outputDataSources = outputs.computeIfAbsent(name, k -> new ArrayList<>());
        if (channels == null || channels.length == 0) {
            return;
        }
        Collections.addAll(outputDataSources, channels);
    }

    /**
     * 增加输出channel，对所有输出都适用
     *
     * @param channels
     */
    public static void addChannel(ISink<?>... channels) {
        if (channels == null) {
            return;
        }
        for (ISink<?> outputDataSource : channels) {
            if (outputDataSource != null) {
                defaultOutput.add(outputDataSource);
            }
        }
    }

    public static List<ISink<?>> getOutputDataSource(String name, String level) {
        List<ISink<?>> outputDataSources = outputs.get(name);
        if (outputDataSources != null) {
            return outputDataSources;
        }
        if (defaultOutput != null && defaultOutput.size() > 0) {
            return defaultOutput;
        }
        return null;
    }

    public static void finishMonitor(String name) {
        monitorICache.put(name, null);
    }

    public static void initLogDir(String rootLogDir) {
        LOG_ROOT_DIR = rootLogDir;
    }

    public static ISink<?> createOrGetLogOutputDatasource(String outputName) {
        ISink<?> loggerOutputDataSource = loggerOutputChannelMap.get(outputName);
        if (loggerOutputDataSource != null) {
            return loggerOutputDataSource;
        }
        synchronized (MonitorFactory.class) {
            loggerOutputDataSource = loggerOutputChannelMap.get(outputName);
            if (loggerOutputDataSource != null) {
                return loggerOutputDataSource;
            }
            loggerOutputDataSource = new LoggerOutputChannel(LOG_ROOT_DIR, outputName);

            loggerOutputChannelMap.put(outputName, loggerOutputDataSource);
            ((ITaskStart) loggerOutputDataSource).startTask();
            loggerOutputDataSource.openAutoFlush();
            return loggerOutputDataSource;

        }

    }
}
