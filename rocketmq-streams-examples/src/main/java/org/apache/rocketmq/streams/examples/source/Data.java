package org.apache.rocketmq.streams.examples.source;

import java.io.Serializable;

public class Data implements Serializable {

    private Integer inFlow;

    private String projectName;

    private String logStore;

    private Integer outFlow;

    public Integer getInFlow() {
        return inFlow;
    }

    public void setInFlow(Integer inFlow) {
        this.inFlow = inFlow;
    }

    public String getProjectName() {
        return projectName;
    }

    public void setProjectName(String projectName) {
        this.projectName = projectName;
    }

    public String getLogStore() {
        return logStore;
    }

    public void setLogStore(String logStore) {
        this.logStore = logStore;
    }

    public Integer getOutFlow() {
        return outFlow;
    }

    public void setOutFlow(Integer outFlow) {
        this.outFlow = outFlow;
    }

    @Override
    public String toString() {
        return "Data{" +
            "inFlow=" + inFlow +
            ", projectName='" + projectName + '\'' +
            ", logStore='" + logStore + '\'' +
            ", outFlow=" + outFlow +
            '}';
    }
}

