package org.apache.rocketmq.streams.common.monitor.service;

import java.util.Collection;
import java.util.List;
import org.apache.rocketmq.streams.common.monitor.model.JobStage;
import org.apache.rocketmq.streams.common.monitor.model.TraceIdsDO;
import org.apache.rocketmq.streams.common.monitor.model.TraceMonitorDO;

public interface MonitorDataSyncService {

    List<TraceIdsDO> getTraceIds();

    void updateJobStage(Collection<JobStage> jobStages);

    void addTraceMonitor(TraceMonitorDO traceMonitorDO);
}
