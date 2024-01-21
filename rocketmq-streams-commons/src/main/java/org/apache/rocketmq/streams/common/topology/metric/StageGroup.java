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
package org.apache.rocketmq.streams.common.topology.metric;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;

public class StageGroup extends BasedConfigurable {
    public static String TYPE = "StageGroup";

    /**
     * 展示在拓扑中
     */
    protected String viewName;
    /**
     * stage info
     */
    protected String startLabel;
    protected String endLabel;
    protected List<String> allStageLabels;

    protected transient AbstractStage<?> startStage;
    protected transient AbstractStage<?> endStage;
    protected transient List<AbstractStage<?>> allStages;

    protected List<String> childrenNames = new ArrayList<>();
    protected String parentName;
    protected transient List<StageGroup> children = new ArrayList<>();
    protected transient StageGroup parent;

    /**
     * SQL info
     */

    protected String sql;
    protected String subTableName;

    /**
     * metric info
     */

    protected double qps;//1分钟qps
    protected long inCount;
    protected long outCount;
    protected long avgCost;
    protected long maxCost;

    public StageGroup() {
        setType(TYPE);
    }

    public StageGroup(AbstractStage<?> startStage, AbstractStage<?> endStage, List<AbstractStage<?>> allStages) {
        setType(TYPE);

        this.setName(NameCreatorContext.get().createName("StageGroup"));
        //this.setNameSpace(namespace);
        setStartStage(startStage);
        setEndStage(endStage);
        if (allStages != null) {
            List<String> allStageLabels = new ArrayList<>();
            for (AbstractStage<?> stage : allStages) {
                allStageLabels.add(stage.getLabel());
            }
            this.allStageLabels = allStageLabels;
            this.allStages = allStages;
        }
    }

    /**
     * 在序列化时做反序列化
     *
     * @param stageMap
     */
    public void init(Map<String, AbstractStage<?>> stageMap, Map<String, StageGroup> stageGroupMap) {
        this.startStage = stageMap.get(this.startLabel);
        this.endStage = stageMap.get(this.endLabel);
        this.parent = stageGroupMap.get(this.parentName);
        List<StageGroup> children = new ArrayList<>();
        for (String name : this.childrenNames) {
            children.add(stageGroupMap.get(name));
        }
        this.children = children;

        List<AbstractStage<?>> allStages = new ArrayList<>();
        List<String> tmpStageLabels = new ArrayList<>();
        tmpStageLabels.addAll(this.allStageLabels);
        Iterator<String> it = tmpStageLabels.iterator();
        while (it.hasNext()) {
            String name = it.next();
            AbstractStage<?> stage = stageMap.get(name);
            allStages.add(stage);
            stage.setStageGroup(this);
        }
        this.allStages = allStages;
    }

    public void calculateMetric() {
        inCount = startStage.calculateInCount();
        qps = startStage.calculateInQPS();
        if (children != null) {
            for (StageGroup stageGroup : children) {
                if (stageGroup.getStartStage().calculateInCount() > inCount) {
                    inCount = stageGroup.getStartStage().calculateInCount();
                }
                if (stageGroup.getStartStage().calculateInQPS() > qps) {
                    qps = stageGroup.getStartStage().calculateInQPS();
                }
            }
        }
        outCount = endStage.getStageMetric().getOutCount();
    }

    public String getStartLabel() {
        return startLabel;
    }

    public void setStartLabel(String startLabel) {
        this.startLabel = startLabel;
    }

    public String getEndLabel() {
        return endLabel;
    }

    public void setEndLabel(String endLabel) {
        this.endLabel = endLabel;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getSubTableName() {
        return subTableName;
    }

    public void setSubTableName(String subTableName) {
        this.subTableName = subTableName;
    }

    public double getQps() {
        return qps;
    }

    public long getInCount() {
        return inCount;
    }

    public long getOutCount() {
        return outCount;
    }

    public long getAvgCost() {
        return avgCost;
    }

    public long getMaxCost() {
        return maxCost;
    }

    public StageGroup getParent() {
        return parent;
    }

    public void setParent(StageGroup parent) {
        parent.children.add(this);
        this.parent = parent;
        this.parentName = this.parent.getName();
        parent.childrenNames.add(this.getName());
    }

    public List<String> getChildrenNames() {
        return childrenNames;
    }

    public void setChildrenNames(List<String> childrenNames) {
        this.childrenNames = childrenNames;
    }

    public String getParentName() {
        return parentName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public AbstractStage<?> getStartStage() {
        return startStage;
    }

    public void setStartStage(AbstractStage<?> startStage) {
        this.startStage = startStage;
        this.startLabel = startStage.getLabel();
    }

    public AbstractStage<?> getEndStage() {
        return endStage;
    }

    public void setEndStage(AbstractStage<?> endStage) {
        this.endStage = endStage;
        this.endLabel = endStage.getLabel();
    }

    public List<StageGroup> getChildren() {
        return children;
    }

    public void setChildren(List<StageGroup> children) {
        this.children = children;
    }

    public List<String> getAllStageLabels() {
        return allStageLabels;
    }

    public void setAllStageLabels(List<String> allStageLabels) {
        this.allStageLabels = allStageLabels;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public List<AbstractStage<?>> getAllStages() {
        return allStages;
    }
}
