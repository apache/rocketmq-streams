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

public class JobStage extends BasedConfigurable {

    /**
     * 机器名称
     */
    private String machineName;

    /**
     * stage名称
     */
    private String stageName;

    /**
     * stage类型
     */
    private String stageType;

    /**
     * stage下游集合
     */
    private String nextStageLables;

    /**
     * stage上游集合
     */
    private String prevStageLables;

    /**
     * stage内容
     */
    private String stageContent;
    private String sqlContent;

    public String getMachineName() {
        return machineName;
    }

    public void setMachineName(String machineName) {
        this.machineName = machineName;
    }

    public String getStageName() {
        return stageName;
    }

    public void setStageName(String stageName) {
        this.stageName = stageName;
    }

    public String getStageType() {
        return stageType;
    }

    public void setStageType(String stageType) {
        this.stageType = stageType;
    }

    public String getNextStageLables() {
        return nextStageLables;
    }

    public void setNextStageLables(String nextStageLables) {
        this.nextStageLables = nextStageLables;
    }

    public String getPrevStageLables() {
        return prevStageLables;
    }

    public void setPrevStageLables(String prevStageLables) {
        this.prevStageLables = prevStageLables;
    }

    public String getStageContent() {
        return stageContent;
    }

    public void setStageContent(String stageContent) {
        this.stageContent = stageContent;
    }

    public String getSqlContent() {
        return sqlContent;
    }

    public void setSqlContent(String sqlContent) {
        this.sqlContent = sqlContent;
    }
}
