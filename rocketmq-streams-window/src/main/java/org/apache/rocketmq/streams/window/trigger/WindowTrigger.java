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
package org.apache.rocketmq.streams.window.trigger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.AbstractMultiSplitMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportShuffleSource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.SessionOperator;

public class WindowTrigger extends AbstractSupportShuffleSource implements IStreamOperator {
    protected static final Log LOG = LogFactory.getLog(WindowTrigger.class);
    private AbstractWindow window;
    //这个时间是在于数据很离散，无法触发窗口的时候做的补位
    protected transient Long eventTimeLastUpdateTime;
    protected transient ScheduledExecutorService fireCheckScheduler;//检查窗口实例是否可以触发
    protected transient ConcurrentHashMap<String, WindowInstance> windowInstances = new ConcurrentHashMap();//保存所有注册的窗口实例，多个相同实例注册，只保留一个
    //所有注册的窗口实例，按触发顺序排序，如果触发时间相同，按开始时间排序
    protected transient PriorityQueue<WindowInstance> orderWindowInstancs = new PriorityQueue(new Comparator<WindowInstance>() {
        @Override
        public int compare(WindowInstance o1, WindowInstance o2) {
            int value = o1.getFireTime().compareTo(o2.getFireTime());
            if (value != 0) {
                return value;
            }
            return o2.getStartTime().compareTo(o1.getStartTime());
        }
    });

    //所有可以触发的窗口实例，放到缓存中，有缓存线程调度执行触发逻辑，同一个分片的窗口实例按顺序串行触发，各个分片可以并行
    protected transient MessageCache<WindowInstance> fireInstanceCache = new WindowInstanceCache();
    //正在触发中的windowintance
    protected transient ConcurrentHashMap<String, WindowInstance> firingWindowInstances = new ConcurrentHashMap<>();

    public WindowTrigger(AbstractWindow window) {
        this.window = window;
    }

    @Override
    protected boolean initConfigurable() {
        fireCheckScheduler = new ScheduledThreadPoolExecutor(2);
        setReceiver(window.getFireReceiver());
        fireInstanceCache.openAutoFlush();
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        //检查window instance，如果已经到了触发时间，且符合触发条件，直接触发，如果到了触发时间，还未符合触发条件。则放入触发列表。下次调度时间是下一个最近触发的时间

        fireCheckScheduler.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    if (orderWindowInstancs.size() == 0) {
                        return;

                    }
                    WindowInstance windowInstance = orderWindowInstancs.peek();
                    while (windowInstance != null) {
                        boolean isStartNow = false;
                        if (SessionOperator.SESSION_WINDOW_BEGIN_TIME.equalsIgnoreCase(windowInstance.getStartTime())) {
                            isStartNow = true;
                        }
                        boolean success = executeFireTask(windowInstance, isStartNow);
                        if (success) {
                            windowInstances.remove(windowInstance.createWindowInstanceTriggerId());
                            orderWindowInstancs.remove(windowInstance);
                            windowInstance = orderWindowInstancs.peek();
                        } else {
                            break;
                        }

                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, 10, 1, TimeUnit.SECONDS);

        return false;
    }

    /**
     * 如果没有window instance，则注册，否则放弃
     *
     * @param windowInstance
     */
    public void registFireWindowInstanceIfNotExist(WindowInstance windowInstance, AbstractWindow window) {
        String windowInstanceTriggerId = windowInstance.createWindowInstanceTriggerId();
        WindowInstance old = windowInstances.putIfAbsent(windowInstanceTriggerId, windowInstance);
        if (old == null) {
            window.registerWindowInstance(windowInstance);
            offerWindowInstance(windowInstance);
        }
        LOG.debug("register window instance into manager, instance key: " + windowInstanceTriggerId);
    }

    /**
     * 注册一个window instance
     *
     * @param windowInstance
     */
    public void updateWindowInstanceLastUpdateTime(WindowInstance windowInstance) {
        String windowInstanceTriggerId = windowInstance.createWindowInstanceTriggerId();
        this.eventTimeLastUpdateTime = System.currentTimeMillis();
        WindowInstance old = windowInstances.putIfAbsent(windowInstanceTriggerId, windowInstance);
        if (old == null) {
            offerWindowInstance(windowInstance);
        }
    }

    protected void offerWindowInstance(WindowInstance windowInstance) {
        String triggerId = windowInstance.createWindowInstanceTriggerId();
        if (this.firingWindowInstances.containsKey(triggerId)) {
            return;
        }
        synchronized (this) {
            if (this.firingWindowInstances.containsKey(triggerId)) {
                return;
            }
            this.orderWindowInstancs.offer(windowInstance);
        }

    }

    /**
     * 触发窗口
     *
     * @param windowInstance
     */
    public boolean executeFireTask(WindowInstance windowInstance, boolean startNow) {
        String windowInstanceTriggerId = windowInstance.createWindowInstanceTriggerId();
        FireResult fireResult = canFire(windowInstance);
        if (fireResult.isCanFire()) {
            //maybe firimg
            if (firingWindowInstances.containsKey(windowInstanceTriggerId)) {
                return true;
            }
            //start firing
            DebugWriter.getDebugWriter(window.getConfigureName()).writeFireWindowInstance(windowInstance, eventTimeLastUpdateTime, this.window.getMaxEventTime(windowInstance.getSplitId()), fireResult.getReason());
            firingWindowInstances.put(windowInstanceTriggerId, windowInstance);
            if (startNow) {
                fireWindowInstance(windowInstance);
            } else {
                fireInstanceCache.addCache(windowInstance);
            }
            return true;
        }
        return false;
    }

    /**
     * 触发窗口
     *
     * @param windowInstance
     */
    protected void fireWindowInstance(WindowInstance windowInstance) {
        try {
            if (windowInstance == null) {
                LOG.error("window instance is null!");
                return;
            }
            String windowInstanceTriggerId = windowInstance.createWindowInstanceTriggerId();
            if (window == null) {
                LOG.error(windowInstanceTriggerId + "'s window object have been removed!");
                return;
            }

            if (windowInstance.getLastMaxUpdateTime() == null) {
                windowInstance.setLastMaxUpdateTime(window.getMaxEventTime(windowInstance.getSplitId()));
            }
            int fireCount = window.fireWindowInstance(windowInstance);
            LOG.debug("fire instance(" + windowInstanceTriggerId + " fire count is " + fireCount);
            firingWindowInstances.remove(windowInstanceTriggerId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 是否符合触发条件
     *
     * @param windowInstance
     * @return
     */
    protected FireResult canFire(WindowInstance windowInstance) {
        String windowInstanceTriggerId = windowInstance.createWindowInstanceTriggerId();
        if (window == null) {
            LOG.warn(windowInstanceTriggerId + " can't find window!");
            return new FireResult();
        }
        Date fireTime = DateUtil.parseTime(windowInstance.getFireTime());
        Boolean isTest = ComponentCreator.getPropertyBooleanValue("window.fire.isTest");
        if (isTest) {
            if (System.currentTimeMillis() - fireTime.getTime() > 0) {
                System.out.println(windowInstance.getWindowName() + " is fired by test timeout");
                return new FireResult(true, 3);
            }
        }
        /**
         * 未到触发时间
         */
        Long maxEventTime = this.window.getMaxEventTime(windowInstance.getSplitId());
        if (window.getTimeFieldName() == null) {
            maxEventTime = System.currentTimeMillis();
        }
        if (maxEventTime != null && maxEventTime - fireTime.getTime() >= 3000) {
            return new FireResult(true, 0);
        }
        Long eventTimeLastUpdateTime = this.eventTimeLastUpdateTime;
        if (eventTimeLastUpdateTime == null) {
            return new FireResult();
        }
        if (isTest) {
            int gap = (int) (System.currentTimeMillis() - eventTimeLastUpdateTime);
            if (window.getMsgMaxGapSecond() != null && gap > window.getMsgMaxGapSecond() * 1000) {
                LOG.warn("the fire reason is exceed the gap " + gap + " window instance id is " + windowInstanceTriggerId);
                return new FireResult(true, 1);
            }
        }
        return new FireResult();
    }

    @Override
    public Object doMessage(IMessage message, AbstractContext context) {
        return null;
    }

    public synchronized void fireWindowInstance(String queueId) {
        List<WindowInstance> windowInstanceList = new ArrayList<>();
        ConcurrentHashMap<String, WindowInstance> newWindowInstanceMap = new ConcurrentHashMap();
        for (String key : windowInstances.keySet()) {
            WindowInstance windowInstance = windowInstances.get(key);
            if (windowInstance.getSplitId().equals(queueId)) {
                windowInstanceList.add(windowInstance);
            } else {
                newWindowInstanceMap.put(key, windowInstance);
            }

        }
        windowInstances = newWindowInstanceMap;
        Collections.sort(windowInstanceList, new Comparator<WindowInstance>() {
            @Override
            public int compare(WindowInstance o1, WindowInstance o2) {
                int value = o1.getFireTime().compareTo(o2.getFireTime());
                if (value != 0) {
                    return value;
                }
                return o2.getStartTime().compareTo(o1.getStartTime());
            }
        });
        for (WindowInstance windowInstance : windowInstanceList) {
            fireWindowInstance(windowInstance);
        }
    }

    protected class WindowInstanceCache extends AbstractMultiSplitMessageCache<WindowInstance> {

        public WindowInstanceCache() {
            super(new IMessageFlushCallBack<WindowInstance>() {
                @Override
                public boolean flushMessage(List<WindowInstance> windowInstances) {

                    Collections.sort(windowInstances, new Comparator<WindowInstance>() {
                        @Override
                        public int compare(WindowInstance o1, WindowInstance o2) {
                            int value = o1.getFireTime().compareTo(o2.getFireTime());
                            if (value != 0) {
                                return value;
                            }
                            return o2.getStartTime().compareTo(o1.getStartTime());
                        }
                    });

                    for (WindowInstance windowInstance : windowInstances) {
                        fireWindowInstance(windowInstance);
                    }
                    return true;
                }
            });
        }

        @Override
        protected String createSplitId(WindowInstance windowInstance) {
            return windowInstance.getSplitId();
        }
    }

    protected class FireResult {
        protected boolean canFire = false;
        protected int reason = -1;//0:event time;1:timeout;-1:nothign

        public FireResult(boolean canFire, int reason) {
            this.canFire = canFire;
            this.reason = reason;
        }

        public FireResult() {
            this.canFire = false;
            this.reason = -1;
        }

        public boolean isCanFire() {
            return canFire;
        }

        public int getReason() {
            return reason;
        }
    }

    @Override
    public boolean supportNewSplitFind() {
        return true;
    }

    @Override
    public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override
    public boolean supportOffsetRest() {
        return false;
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return false;
    }
}
