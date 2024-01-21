package org.apache.rocketmq.streams.common.threadpool;

import com.google.common.collect.Maps;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;

public class ScheduleFactory {

    private final ScheduledExecutorService scheduledExecutorService;

    private final Map<String, ScheduledFuture<?>> scheduledFutureMap;

    private ScheduleFactory() {
        this.scheduledExecutorService = new ScheduledThreadPoolExecutor(10, new BasicThreadFactory.Builder().namingPattern("rstream-schedule-%d").build());
        this.scheduledFutureMap = Maps.newConcurrentMap();
    }

    public static ScheduleFactory getInstance() {
        return ScheduleManager.Instance;
    }

    public void execute(String scheduleName, Runnable command, long initialDelay, long delay, TimeUnit unit) {
        ScheduledFuture<?> scheduledFuture = this.scheduledExecutorService.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        this.scheduledFutureMap.put(scheduleName, scheduledFuture);
    }

    public void cancel(String scheduleName) {
        ScheduledFuture<?> scheduledFuture = this.scheduledFutureMap.get(scheduleName);
        if (scheduledFuture != null && !scheduledFuture.isDone()) {
            scheduledFuture.cancel(false);
            this.scheduledFutureMap.remove(scheduleName);
        }
    }

    private static class ScheduleManager {
        private static final ScheduleFactory Instance = new ScheduleFactory();
    }

}
