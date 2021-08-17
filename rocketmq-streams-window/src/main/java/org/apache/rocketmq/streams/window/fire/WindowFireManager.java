package org.apache.rocketmq.streams.window.fire;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class WindowFireManager {
    private AbstractWindow window;
    /**
     * splitId,max
     */
    protected transient ConcurrentHashMap<String,Long> maxEventTimes=new ConcurrentHashMap();//max event time procced by window
    protected transient ConcurrentHashMap<String,Long> eventTimeLastUpdateTimes=new ConcurrentHashMap<>();
}
