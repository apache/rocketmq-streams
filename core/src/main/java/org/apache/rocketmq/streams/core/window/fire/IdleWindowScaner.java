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
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class IdleWindowScaner implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(IdleWindowScaner.class.getName());

    private final Integer maxIdleTime;
    private final ScheduledExecutorService executor;

    private final ConcurrentHashMap<WindowKey, TimeType> lastUpdateTime2WindowKey = new ConcurrentHashMap<>(16);

    private final ConcurrentHashMap<WindowKey, AccumulatorWindowFire<?, ?, ?, ?>> fireWindowCallBack = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AccumulatorSessionWindowFire<?, ?, ?, ?>> fireSessionWindowCallback = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AggregateWindowFire<?, ?, ?>> windowKeyAggregate = new ConcurrentHashMap<>(16);
    private final ConcurrentHashMap<WindowKey, AggregateSessionWindowFire<?, ?, ?>> windowKeyAggregateSession = new ConcurrentHashMap<>(16);

    private final ConcurrentHashMap<WindowKey, JoinWindowFire<?, ?, ?, ?>> fireJoinWindowCallback = new ConcurrentHashMap<>(16);


    public IdleWindowScaner(Integer maxIdleTime, ScheduledExecutorService executor) {
        this.maxIdleTime = maxIdleTime;
        this.executor = executor;
        this.executor.scheduleAtFixedRate(() -> {
            try {
                scanAndFireWindow();
            } catch (Throwable t) {
                logger.error("scan and fire the idle window error.", t);
            }
        }, 0, 1000, TimeUnit.MILLISECONDS);
    }

    public void putAccumulatorWindowCallback(WindowKey windowKey, long watermark, AccumulatorWindowFire<?, ?, ?, ?> function) {
        this.fireWindowCallBack.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AccumulatorWindow, System.currentTimeMillis(), watermark);
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
                timeType.setWatermark(watermark);
            }
            return timeType;
        });
    }

    public void putAccumulatorSessionWindowCallback(WindowKey windowKey, long watermark, AccumulatorSessionWindowFire<?, ?, ?, ?> function) {
        this.fireSessionWindowCallback.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AccumulatorSessionWindow, System.currentTimeMillis(), watermark);
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
                timeType.setWatermark(watermark);
            }
            return timeType;
        });
    }

    public void putAggregateWindowCallback(WindowKey windowKey, long watermark, AggregateWindowFire<?, ?, ?> function) {
        this.windowKeyAggregate.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AggregateWindow, System.currentTimeMillis(), watermark);
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
                timeType.setWatermark(watermark);
            }
            return timeType;
        });
    }

    public void putAggregateSessionWindowCallback(WindowKey windowKey, long watermark, AggregateSessionWindowFire<?, ?, ?> function) {
        this.windowKeyAggregateSession.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.AggregateSessionWindow, System.currentTimeMillis(), watermark);
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
                timeType.setWatermark(watermark);
            }
            return timeType;
        });
    }

    public void putJoinWindowCallback(WindowKey windowKey, long watermark, JoinWindowFire<?, ?, ?, ?> function) {
        this.fireJoinWindowCallback.putIfAbsent(windowKey, function);
        this.lastUpdateTime2WindowKey.compute(windowKey, (key, timeType) -> {
            if (timeType == null) {
                timeType = new TimeType(Type.JoinWindow, System.currentTimeMillis(), watermark);
            } else {
                timeType.setUpdateTime(System.currentTimeMillis());
                timeType.setWatermark(watermark);
            }
            return timeType;
        });
    }

    public void removeOldAccumulatorSession(WindowKey oldWindowKey) {
        if (oldWindowKey == null) {
            return;
        }

        TimeType timeType = this.lastUpdateTime2WindowKey.get(oldWindowKey);
        if (timeType != null && timeType.getType() == Type.AccumulatorSessionWindow) {
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

    private void scanAndFireWindow() throws Throwable {
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
                    long watermark = timeType.getWatermark() + idleTime;
                    if (watermark > windowKey.getWindowEnd()) {
                        try {
                            doFire(windowKey, type, watermark);
                        } finally {
                            iterator.remove();
                        }
                    }
                    break;
                }
                case AccumulatorWindow:
                case JoinWindow:
                case AggregateWindow: {
                    long watermark = timeType.getWatermark() + idleTime;
                    if (idleTime > this.maxIdleTime && watermark > windowKey.getWindowEnd()) {
                        try {
                            doFire(windowKey, type, watermark);
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

    private void doFire(WindowKey windowKey, Type type, long watermark) throws Throwable {
        String operatorName = windowKey.getOperatorName();

        switch (type) {
            case AccumulatorWindow: {
                AccumulatorWindowFire<?, ?, ?, ?> func = this.fireWindowCallBack.remove(windowKey);
                if (func != null) {
                    //write the result out, delete the state from local and remote
                    func.fire(operatorName, watermark);
                    //commit watermark to local and remote.
                    func.commitWatermark(watermark);
                }
                break;
            }
            case AccumulatorSessionWindow: {
                AccumulatorSessionWindowFire<?, ?, ?, ?> accumulatorSessionWindowFire = this.fireSessionWindowCallback.remove(windowKey);
                if (accumulatorSessionWindowFire != null) {
                    accumulatorSessionWindowFire.fire(operatorName, watermark);
                    accumulatorSessionWindowFire.commitWatermark(watermark);
                }
                break;
            }
            case AggregateWindow: {
                AggregateWindowFire<?, ?, ?> aggregateWindowFire = this.windowKeyAggregate.remove(windowKey);
                if (aggregateWindowFire != null) {
                    aggregateWindowFire.fire(operatorName, watermark);
                    aggregateWindowFire.commitWatermark(watermark);
                }
                break;
            }
            case AggregateSessionWindow: {
                AggregateSessionWindowFire<?, ?, ?> sessionWindowFire = this.windowKeyAggregateSession.remove(windowKey);
                if (sessionWindowFire != null) {
                    sessionWindowFire.fire(operatorName, watermark);
                    sessionWindowFire.commitWatermark(watermark);
                }
                break;
            }
            case JoinWindow: {
                JoinWindowFire<?, ?, ?, ?> joinWindowFire = this.fireJoinWindowCallback.remove(windowKey);
                if (joinWindowFire != null) {
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
        private long watermark;

        public TimeType(Type type, long updateTime, long watermark) {
            this.type = type;
            this.updateTime = updateTime;
            this.watermark = watermark;
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

        public long getWatermark() {
            return watermark;
        }

        public void setWatermark(long watermark) {
            this.watermark = watermark;
        }
    }

    enum Type {
        AccumulatorWindow, AccumulatorSessionWindow, AggregateWindow, AggregateSessionWindow, JoinWindow
    }
}
