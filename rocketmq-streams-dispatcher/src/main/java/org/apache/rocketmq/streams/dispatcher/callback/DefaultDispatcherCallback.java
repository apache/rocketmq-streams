package org.apache.rocketmq.streams.dispatcher.callback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.topology.graph.JobGraph;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;

public class DefaultDispatcherCallback implements IDispatcherCallback<JobGraph> {

    private final Map<String, JobGraph> jobGraphMap = Maps.newHashMap();

    @Override public List<String> start(List<String> jobNames) {
        List<String> success = Lists.newArrayList();
        for (String jobName : jobNames) {
            if (jobGraphMap.containsKey(jobName)) {
                jobGraphMap.get(jobName).start();
                success.add(jobName);
            }
        }
        return success;
    }

    @Override public List<String> stop(List<String> jobNames) {
        List<String> success = Lists.newArrayList();
        for (String jobName : jobNames) {
            if (jobGraphMap.containsKey(jobName)) {
                jobGraphMap.get(jobName).stop();
                success.add(jobName);
            }
        }
        return success;
    }

    @Override public List<String> list() {
        return Lists.newArrayList(jobGraphMap.keySet());
    }

    public void add(String jobName, JobGraph job) {
        this.jobGraphMap.put(jobName, job);
    }

    public void delete(String jobName) {
        this.jobGraphMap.remove(jobName);
    }

    @Override
    public void destroy() {
        this.jobGraphMap.clear();
    }
}
