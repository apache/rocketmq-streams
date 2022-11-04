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
package org.apache.rocketmq.streams.common.model;

import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

public class StageRelation extends BasedConfigurable {
    private String jobName;

    private String viewName;

    private String groupName;

    private String sqlContent;

    private String startLabel;

    private String endLabel;

    private String stageLabels;

    private String childrenNames;

    private String parentName;

    private String userId;

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getViewName() {
        return viewName;
    }

    public void setViewName(String viewName) {
        this.viewName = viewName;
    }

    public String getGroupName() {
        return groupName;
    }

    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    public String getSqlContent() {
        return sqlContent;
    }

    public void setSqlContent(String sqlContent) {
        this.sqlContent = sqlContent;
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

    public String getStageLabels() {
        return stageLabels;
    }

    public void setStageLabels(String stageLabels) {
        this.stageLabels = stageLabels;
    }

    public String getChildrenNames() {
        return childrenNames;
    }

    public void setChildrenNames(String childrenNames) {
        this.childrenNames = childrenNames;
    }

    public String getParentName() {
        return parentName;
    }

    public void setParentName(String parentName) {
        this.parentName = parentName;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }
}
