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

import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.window.fire.AggregateSessionWindowFire;
import org.apache.rocketmq.streams.core.window.fire.AggregateWindowFire;
import org.apache.rocketmq.streams.core.window.fire.JoinWindowFire;
import org.apache.rocketmq.streams.core.window.fire.AccumulatorWindowFire;
import org.apache.rocketmq.streams.core.window.fire.AccumulatorSessionWindowFire;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;


public class IdleWindowScaner implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IdleWindowScaner.class.getName());

    private final Integer idleTime;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "ScanIdleWindowThread"));

    private final ConcurrentHashMap<WindowKey, TimeType> lastUpdateTime2WindowKey = new ConcurrentHashMap<>(16);

    private final ConcurrentHashMap<WindowKey, AccumulatorWindowFire<?, ?, ?, ?>> fireWindowCallBack = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AccumulatorSessionWindowFire<?, ?, ?, ?>> fireSessionWindowCallback = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AggregateWindowFire<?, ?, ?>> windowKeyAggregate = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AggregateSessionWindowFire<?, ?, ?>> windowKeyAggregateSession = new ConcurrentHashMap<>(16);

    private final ConcurrentHashMap<WindowKey, JoinWindowFire<?, ?, ?, ?>> fireJoinWindowCallback = new ConcurrentHashMap<>(16);


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

    public void putAccumulatorWindowCallback(WindowKey windowKey, AccumulatorWindowFire<?, ?, ?, ?> function) {
        this.fireWindowCallBack.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AccumulatorWindow, System.currentTimeMillis());
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
            }
            return timeType;
        });
    }

    public void putAccumulatorSessionWindowCallback(WindowKey windowKey, AccumulatorSessionWindowFire<?, ?, ?, ?> function) {
        this.fireSessionWindowCallback.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AccumulatorSessionWindow, System.currentTimeMillis());
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
            }
            return timeType;
        });
    }

    public void putAggregateWindowCallback(WindowKey windowKey, AggregateWindowFire<?, ?, ?> function) {
        this.windowKeyAggregate.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AggregateWindow, System.currentTimeMillis());
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
            }
            return timeType;
        });
    }

    public void putAggregateSessionWindowCallback(WindowKey windowKey, AggregateSessionWindowFire<?, ?, ?> function) {
        this.windowKeyAggregateSession.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AggregateSessionWindow, System.currentTimeMillis());
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
            }
            return timeType;
        });
    }

    public void putJoinWindowCallback(WindowKey windowKey, JoinWindowFire<?, ?, ?, ?> function) {
        this.fireJoinWindowCallback.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.JoinWindow, System.currentTimeMillis());
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
            }
            return timeType;
        });
    }


    public void removeWindowKey(WindowKey windowKey) {
        fireWindowCallBack.remove(windowKey);
        fireSessionWindowCallback.remove(windowKey);
        fireJoinWindowCallback.remove(windowKey);
    }

    private void scanAndFireWindow() {
        Iterator<Map.Entry<WindowKey, TimeType>> iterator = this.lastUpdateTime2WindowKey.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<WindowKey, TimeType> next = iterator.next();

            WindowKey windowKey = next.getKey();
            TimeType timeType = next.getValue();

            Type type = timeType.getType();
            long updateTime = timeType.getUpdateTime();

            if (System.currentTimeMillis() - updateTime > idleTime) {
                doFire(windowKey, type);
                iterator.remove();
            }
        }
    }

    private void doFire(WindowKey windowKey, Type type) {
        long watermark = windowKey.getWindowEnd() + 1;
        String operatorName = windowKey.getOperatorName();

        switch (type) {
            case AccumulatorWindow: {
                AccumulatorWindowFire<?, ?, ?, ?> func = this.fireWindowCallBack.get(windowKey);
                if (func != null) {
                    logger.debug("fire the accumulator window, with watermark={}, operatorName={}", watermark, operatorName);
                    func.fire(operatorName, watermark);
                }
                break;
            }
            case AccumulatorSessionWindow: {
                AccumulatorSessionWindowFire<?, ?, ?, ?> accumulatorSessionWindowFire = this.fireSessionWindowCallback.get(windowKey);
                if (accumulatorSessionWindowFire != null) {
                    logger.debug("fire the accumulator session window, with windowKey={}", windowKey);
                    accumulatorSessionWindowFire.fire(operatorName, watermark);
                }
                break;
            }
            case AggregateWindow: {
                AggregateWindowFire<?, ?, ?> aggregateWindowFire = this.windowKeyAggregate.get(windowKey);
                if (aggregateWindowFire != null) {
                    logger.debug("fire the aggregate window, with windowKey={}", windowKey);
                    aggregateWindowFire.fire(operatorName, watermark);
                }
                break;
            }
            case AggregateSessionWindow: {
                AggregateSessionWindowFire<?, ?, ?> sessionWindowFire = this.windowKeyAggregateSession.get(windowKey);
                if (sessionWindowFire != null) {
                    logger.debug("fire the aggregate session window, with windowKey={}", windowKey);
                    sessionWindowFire.fire(operatorName, watermark);
                }
                break;
            }
            case JoinWindow: {
                JoinWindowFire<?, ?, ?, ?> joinWindowFire = this.fireJoinWindowCallback.get(windowKey);
                if (joinWindowFire != null) {
                    logger.debug("fire the join window, with watermark={}", watermark);

                    String name = operatorName.substring(0, operatorName.lastIndexOf(Constant.SPLIT));
                    String streamType = operatorName.substring(operatorName.lastIndexOf(Constant.SPLIT) + 1);

                    joinWindowFire.fire(name, watermark, StreamType.valueOf(streamType));
                }
                break;
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.executor.shutdown();
    }

    static class TimeType {
        private Type type;
        private long updateTime;

        public TimeType(Type type, long updateTime) {
            this.type = type;
            this.updateTime = updateTime;
        }

        public Type getType() {
            return type;
        }

        public void setType(Type type) {
            this.type = type;
        }

        public long getUpdateTime() {
            return updateTime;
        }

        public void setUpdateTime(long updateTime) {
            this.updateTime = updateTime;
        }
    }

    enum Type {
        AccumulatorWindow, AccumulatorSessionWindow, AggregateWindow, AggregateSessionWindow, JoinWindow
    }
}
