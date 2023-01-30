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
package org.apache.rocketmq.streams.core.window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;


public class IdleWindowScaner implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IdleWindowScaner.class.getName());

    private final Integer idleTime;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "ScanIdleWindowThread"));

    private final ConcurrentHashMap<WindowKey, Long> lastUpdateTime2WindowKey = new ConcurrentHashMap<>(16);

    private final ConcurrentHashMap<WindowKey, BiConsumer<Long, String>> fireWindowCallBack = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, Consumer<WindowKey>> fireSessionWindowCallback = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, LongConsumer> fireJoinWindowCallback = new ConcurrentHashMap<>(16);


    public IdleWindowScaner(Integer idleTime) {
        this.idleTime = idleTime;
        this.executor.scheduleAtFixedRate(() -> {
            try {
                scanAndFireWindow();
            } catch (Throwable t) {
                logger.error("scan and fire the idle window error.", t);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void putNormalWindowCallback(WindowKey windowKey, BiConsumer<Long, String> function) {
        this.fireWindowCallBack.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.put(windowKey, System.currentTimeMillis());
    }

    public void putSessionWindowCallback(WindowKey windowKey, Consumer<WindowKey> function) {
        this.fireSessionWindowCallback.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.put(windowKey, System.currentTimeMillis());
    }

    public void putJoinWindowCallback(WindowKey windowKey, LongConsumer function) {
        this.fireJoinWindowCallback.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.put(windowKey, System.currentTimeMillis());
    }


    public void removeWindowKey(WindowKey windowKey) {
        fireWindowCallBack.remove(windowKey);
        fireSessionWindowCallback.remove(windowKey);
        fireJoinWindowCallback.remove(windowKey);
    }

    private void scanAndFireWindow() {
        Iterator<Map.Entry<WindowKey, Long>> iterator = this.lastUpdateTime2WindowKey.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<WindowKey, Long> next = iterator.next();

            WindowKey windowKey = next.getKey();
            Long lastUpdate = next.getValue();

            if (System.currentTimeMillis() - lastUpdate > idleTime) {
                doFire(windowKey);
                iterator.remove();
            }
        }
    }

    private void doFire(WindowKey windowKey) {
        long watermark = windowKey.getWindowEnd() + 1;
        String operatorName = windowKey.getOperatorName();

        BiConsumer<Long, String> function = this.fireWindowCallBack.get(windowKey);
        if (function != null) {
            logger.debug("fire the normal window, with watermark={}, operatorName={}", watermark, operatorName);
            function.accept(watermark, operatorName);
        }

        Consumer<WindowKey> consumer = this.fireSessionWindowCallback.get(windowKey);
        if (consumer != null) {
            logger.debug("fire the session window, with windowKey={}", windowKey);
            consumer.accept(windowKey);
        }

        LongConsumer longConsumer = this.fireJoinWindowCallback.get(windowKey);
        if (longConsumer != null) {
            logger.debug("fire the session window, with watermark={}", watermark);
            longConsumer.accept(watermark);
        }
    }

    @Override
    public void close() throws Exception {
        this.executor.shutdown();
    }
}
