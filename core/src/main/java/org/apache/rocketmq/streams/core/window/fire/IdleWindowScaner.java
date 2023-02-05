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
package org.apache.rocketmq.streams.core.window.fire;

import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.window.StreamType;
import org.apache.rocketmq.streams.core.window.WindowKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class IdleWindowScaner implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IdleWindowScaner.class.getName());

    private final Integer maxIdleTime;
    private long sessionTimeOut = 0;
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "ScanIdleWindowThread"));

    private final ConcurrentHashMap<WindowKey, TimeType> lastUpdateTime2WindowKey = new ConcurrentHashMap<>(16);

    private final ConcurrentHashMap<WindowKey, AccumulatorWindowFire<?, ?, ?, ?>> fireWindowCallBack = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AccumulatorSessionWindowFire<?, ?, ?, ?>> fireSessionWindowCallback = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AggregateWindowFire<?, ?, ?>> windowKeyAggregate = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AggregateSessionWindowFire<?, ?, ?>> windowKeyAggregateSession = new ConcurrentHashMap<>(16);

    private final ConcurrentHashMap<WindowKey, JoinWindowFire<?, ?, ?, ?>> fireJoinWindowCallback = new ConcurrentHashMap<>(16);


    public IdleWindowScaner(Integer maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
        this.executor.scheduleAtFixedRate(() -> {
            try {
                scanAndFireWindow();
            } catch (Throwable t) {
                logger.error("scan and fire the idle window error.", t);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void initSessionTimeOut(long sessionTimeOut) {
        this.sessionTimeOut = sessionTimeOut;
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

    public void removeOldAccumulatorSession(WindowKey oldWindowKey) {
        TimeType timeType = this.lastUpdateTime2WindowKey.get(oldWindowKey);
        if (timeType != null
                && timeType.getType() == Type.AccumulatorSessionWindow) {
            this.lastUpdateTime2WindowKey.remove(oldWindowKey);
        }
        this.fireSessionWindowCallback.remove(oldWindowKey);
    }

    public void removeOldAggregateSession(WindowKey oldWindowKey) {
        TimeType timeType = this.lastUpdateTime2WindowKey.get(oldWindowKey);
        if (timeType != null
                && timeType.getType() == Type.AggregateSessionWindow) {
            this.lastUpdateTime2WindowKey.remove(oldWindowKey);
        }
        this.windowKeyAggregateSession.remove(oldWindowKey);
    }

    public void removeWindowKey(WindowKey windowKey) {
        lastUpdateTime2WindowKey.remove(windowKey);

        fireWindowCallBack.remove(windowKey);
        fireSessionWindowCallback.remove(windowKey);

        windowKeyAggregate.remove(windowKey);
        windowKeyAggregateSession.remove(windowKey);

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

            long idleTime = System.currentTimeMillis() - updateTime;

            switch (type) {
                case AggregateSessionWindow:
                case AccumulatorSessionWindow: {
                    if (idleTime >= sessionTimeOut) {
                        try {
                            doFire(windowKey, type);
                        } finally {
                            iterator.remove();
                        }
                    }
                    break;
                }
                case AccumulatorWindow:
                case JoinWindow:
                case AggregateWindow: {
                    long windowSize = windowKey.getWindowEnd() - windowKey.getWindowStart();
                    if (idleTime > this.maxIdleTime && idleTime > windowSize) {
                        try {
                            doFire(windowKey, type);
                        } finally {
                            iterator.remove();
                        }
                    }
                    break;
                }
                default:
                    throw new UnsupportedOperationException("unknown window type: " + type);
            }
        }
    }

    private void doFire(WindowKey windowKey, Type type) {
        long watermark = windowKey.getWindowEnd() + 1;
        String operatorName = windowKey.getOperatorName();

        switch (type) {
            case AccumulatorWindow: {
                AccumulatorWindowFire<?, ?, ?, ?> func = this.fireWindowCallBack.remove(windowKey);
                if (func != null) {
                    logger.debug("fire the accumulator window, with watermark={}, operatorName={}", watermark, operatorName);
                    func.fire(operatorName, watermark);
                    func.commitWatermark(watermark);
                }
                break;
            }
            case AccumulatorSessionWindow: {
                AccumulatorSessionWindowFire<?, ?, ?, ?> accumulatorSessionWindowFire = this.fireSessionWindowCallback.remove(windowKey);
                if (accumulatorSessionWindowFire != null) {
                    logger.debug("fire the accumulator session window, with windowKey={}", windowKey);
                    accumulatorSessionWindowFire.fire(operatorName, watermark);
                    accumulatorSessionWindowFire.commitWatermark(watermark);
                }
                break;
            }
            case AggregateWindow: {
                AggregateWindowFire<?, ?, ?> aggregateWindowFire = this.windowKeyAggregate.remove(windowKey);
                if (aggregateWindowFire != null) {
                    logger.debug("fire the aggregate window, with windowKey={}", windowKey);
                    aggregateWindowFire.fire(operatorName, watermark);
                    aggregateWindowFire.commitWatermark(watermark);
                }
                break;
            }
            case AggregateSessionWindow: {
                AggregateSessionWindowFire<?, ?, ?> sessionWindowFire = this.windowKeyAggregateSession.remove(windowKey);
                if (sessionWindowFire != null) {
                    logger.debug("fire the aggregate session window, with windowKey={}", windowKey);
                    sessionWindowFire.fire(operatorName, watermark);
                    sessionWindowFire.commitWatermark(watermark);
                }
                break;
            }
            case JoinWindow: {
                JoinWindowFire<?, ?, ?, ?> joinWindowFire = this.fireJoinWindowCallback.remove(windowKey);
                if (joinWindowFire != null) {
                    logger.debug("fire the join window, with watermark={}", watermark);

                    String name = operatorName.substring(0, operatorName.lastIndexOf(Constant.SPLIT));
                    String streamType = operatorName.substring(operatorName.lastIndexOf(Constant.SPLIT) + 1);

                    joinWindowFire.fire(name, watermark, StreamType.valueOf(streamType));
                    joinWindowFire.commitWatermark(watermark);
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
