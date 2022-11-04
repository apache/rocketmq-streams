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
    protected String startLable;
    protected String endLable;
    protected List<String> allStageLables;

    protected transient AbstractStage startStage;
    protected transient AbstractStage endStage;
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

    public StageGroup(AbstractStage startStage, AbstractStage endStage, List<AbstractStage<?>> allStages) {
        setType(TYPE);
        this.setConfigureName(NameCreatorContext.get().createName("StageGroup"));
//        this.setNameSpace(namespace);
        setStartStage(startStage);
        setEndStage(endStage);
        if (allStages != null) {
            List<String> allStageLables = new ArrayList<>();
            for (AbstractStage stage : allStages) {
                allStageLables.add(stage.getLabel());
            }
            this.allStageLables = allStageLables;
            this.allStages = allStages;
        }
    }

    /**
     * 在序列化时做反序列化
     *
     * @param stageMap
     */
    public void init(Map<String, AbstractStage<?>> stageMap, Map<String, StageGroup> stageGroupMap) {
        this.startStage = stageMap.get(this.startLable);
        this.endStage = stageMap.get(this.endLable);
        this.parent = stageGroupMap.get(this.parentName);
        List<StageGroup> children = new ArrayList<>();
        for (String name : this.childrenNames) {
            children.add(stageGroupMap.get(name));
        }
        this.children = children;

        List<AbstractStage<?>> allStages = new ArrayList<>();
        List<String> tmpStageLables = new ArrayList<>();
        tmpStageLables.addAll(this.allStageLables);
        Iterator<String> it = tmpStageLables.iterator();
        while (it.hasNext()) {
            String name = it.next();
            AbstractStage stage = stageMap.get(name);
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

    public String getStartLable() {
        return startLable;
    }

    public void setStartLable(String startLable) {
        this.startLable = startLable;
    }

    public String getEndLable() {
        return endLable;
    }

    public void setEndLable(String endLable) {
        this.endLable = endLable;
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

    public void setStartStage(AbstractStage startStage) {
        this.startStage = startStage;
        this.startLable = startStage.getLabel();
    }

    public void setEndStage(AbstractStage endStage) {
        this.endStage = endStage;
        this.endLable = endStage.getLabel();
    }

    public void setChildren(List<StageGroup> children) {
        this.children = children;
    }

    public StageGroup getParent() {
        return parent;
    }

    public void setParent(StageGroup parent) {
        parent.children.add(this);
        this.parent = parent;
        this.parentName = this.parent.getConfigureName();
        parent.childrenNames.add(this.getConfigureName());
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

    public AbstractStage getStartStage() {
        return startStage;
    }

    public AbstractStage getEndStage() {
        return endStage;
    }

    public List<StageGroup> getChildren() {
        return children;
    }

    public List<String> getAllStageLables() {
        return allStageLables;
    }

    public void setAllStageLables(List<String> allStageLables) {
        this.allStageLables = allStageLables;
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
